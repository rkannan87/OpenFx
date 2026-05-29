package com.enterprise.sftp;

import com.enterprise.sftp.config.SftpTransferProperties;
import com.enterprise.sftp.config.SftpConnectionConfig.PooledSshClient;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SFTP file copy — Java 21, stripped to core transfer only.
 */
@Component
public class SftpFileCopy {

    private static final Logger log = LoggerFactory.getLogger(SftpFileCopy.class);

    private final SftpTransferProperties cfg;
    private final PooledSshClient        sshPool;

    public SftpFileCopy(SftpTransferProperties cfg, PooledSshClient sshPool) {
        this.cfg    = cfg;
        this.sshPool = sshPool;
        log.info("SftpFileCopy ready — sftp={} chunkSize={}MB",
                cfg.enabled(), cfg.chunkSizeBytes() / (1024 * 1024));
    }

    // ── Sealed result hierarchy ───────────────────────────────────────────────

    public sealed interface TransferResult
            permits TransferResult.Success, TransferResult.Failure {

        record Success(long     bytesTransferred,
                       Duration elapsed,
                       double   throughputMBps,
                       String   transferId) implements TransferResult {}

        record Failure(String    reason,
                       Throwable cause,
                       String    transferId) implements TransferResult {}
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public TransferResult copyFile(String sourceFile,
                                   String destinationPath,
                                   String destinationFile) {

        final String  transferId = UUID.randomUUID().toString();
        final Instant start      = Instant.now();
        final String  separator  = StringUtils.endsWith(destinationPath, File.separator)
                ? "" : File.separator;

        log.info("[{}] Transfer start — src={} dst={}/{}",
                transferId, sourceFile, destinationPath, destinationFile);

        return cfg.enabled()
                ? transferViaSftp(sourceFile, destinationPath, destinationFile,
                                  separator, transferId, start)
                : transferLocal(sourceFile, destinationPath, destinationFile,
                                separator, transferId, start);
    }

    // ── SFTP dispatch ─────────────────────────────────────────────────────────

    private TransferResult transferViaSftp(String sourceFile,
                                           String destinationPath,
                                           String destinationFile,
                                           String separator,
                                           String transferId,
                                           Instant start) {
        Path sourcePath = Path.of(sourceFile);
        long fileSize;
        try {
            fileSize = Files.size(sourcePath);
        } catch (IOException ex) {
            return new TransferResult.Failure("Cannot read source file size", ex, transferId);
        }

        String remoteDest = destinationPath + separator + destinationFile;

        return cfg.requiresChunkedTransfer(fileSize)
                ? chunkedParallelTransfer(sourcePath, fileSize, remoteDest, transferId, start)
                : singleStreamTransfer(sourcePath, fileSize, remoteDest, transferId, start);
    }

    // ── Single-stream transfer ────────────────────────────────────────────────

    private TransferResult singleStreamTransfer(Path    sourcePath,
                                                long    fileSize,
                                                String  remoteDest,
                                                String  transferId,
                                                Instant start) {
        SSHClient  ssh  = null;
        SFTPClient sftp = null;
        try {
            ssh  = sshPool.borrowClient();
            sftp = ssh.newSFTPClient();

            try (RemoteFile remoteFile = sftp.open(
                         remoteDest, EnumSet.of(OpenMode.WRITE, OpenMode.CREAT));
                 InputStream  in  = new BufferedInputStream(
                         Files.newInputStream(sourcePath), cfg.bufferSizeBytes());
                 OutputStream out = new BufferedOutputStream(
                         remoteFile.new RemoteFileOutputStream(), cfg.bufferSizeBytes())) {

                in.transferTo(out);
                out.flush();
            }

            Duration elapsed = Duration.between(start, Instant.now());
            log.info("[{}] Single-stream complete — {:.2f} MB/s",
                    transferId, toMBps(fileSize, elapsed));
            return new TransferResult.Success(
                    fileSize, elapsed, toMBps(fileSize, elapsed), transferId);

        } catch (Exception ex) {
            log.error("[{}] Single-stream failed", transferId, ex);
            return new TransferResult.Failure("SFTP single-stream error", ex, transferId);
        } finally {
            closeQuietly(sftp);
            if (ssh != null) sshPool.returnClient(ssh);
        }
    }

    // ── Chunked parallel transfer — Java 21 StructuredTaskScope ──────────────

    /**
     * Parallel chunk upload using {@link StructuredTaskScope.ShutdownOnFailure}.
     *
     * <ul>
     *   <li>Each chunk runs on its own virtual thread — SSH pool is the only
     *       concurrency limiter.</li>
     *   <li>Any chunk exception immediately cancels all sibling virtual threads.</li>
     *   <li>{@code scope.joinUntil(deadline)} enforces one wall-clock deadline
     *       across the entire transfer.</li>
     * </ul>
     */
    private TransferResult chunkedParallelTransfer(Path    sourcePath,
                                                   long    fileSize,
                                                   String  remoteDest,
                                                   String  transferId,
                                                   Instant start) {
        int        totalChunks  = cfg.chunkCount(fileSize);
        AtomicLong totalWritten = new AtomicLong();
        Instant    deadline     = start.plus(
                Duration.ofMinutes(cfg.timeoutPerChunkMinutes() * (long) totalChunks));

        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {

            for (int i = 0; i < totalChunks; i++) {
                final int  idx    = i;
                final long offset = (long) i * cfg.chunkSizeBytes();
                final long length = cfg.chunkLength(i, totalChunks, fileSize);

                scope.fork(() -> transferChunk(
                        sourcePath, remoteDest, idx, offset, length,
                        transferId, totalWritten));
            }

            scope.joinUntil(deadline);   // single deadline for all chunks
            scope.throwIfFailed();       // re-throws first chunk exception

            Duration elapsed = Duration.between(start, Instant.now());
            double   mbps    = toMBps(fileSize, elapsed);
            log.info("[{}] Chunked complete — {:.2f} MB/s over {} chunks",
                    transferId, mbps, totalChunks);
            return new TransferResult.Success(fileSize, elapsed, mbps, transferId);

        } catch (ExecutionException ex) {
            log.error("[{}] Chunk failed — all siblings cancelled", transferId, ex.getCause());
            return new TransferResult.Failure(
                    "Chunk transfer failed", ex.getCause(), transferId);
        } catch (TimeoutException ex) {
            log.error("[{}] Transfer deadline exceeded", transferId);
            return new TransferResult.Failure("Transfer deadline exceeded", ex, transferId);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new TransferResult.Failure("Transfer interrupted", ex, transferId);
        }
    }

    // ── One chunk — runs on a virtual thread ──────────────────────────────────

    private Void transferChunk(Path       sourcePath,
                               String     remoteDest,
                               int        chunkIndex,
                               long       offset,
                               long       length,
                               String     transferId,
                               AtomicLong totalWritten) throws Exception {
        SSHClient  ssh  = null;
        SFTPClient sftp = null;
        try {
            ssh  = sshPool.borrowClient();
            sftp = ssh.newSFTPClient();

            try (FileChannel  channel = FileChannel.open(sourcePath, StandardOpenOption.READ);
                 RemoteFile   remote  = sftp.open(
                         remoteDest,
                         EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.APPEND));
                 OutputStream out     = new BufferedOutputStream(
                         remote.new RemoteFileOutputStream(offset), cfg.bufferSizeBytes())) {

                java.nio.MappedByteBuffer mapped =
                        channel.map(FileChannel.MapMode.READ_ONLY, offset, length);

                byte[] buf       = new byte[cfg.bufferSizeBytes()];
                long   remaining = length;

                while (remaining > 0) {
                    int batch = (int) Math.min(cfg.bufferSizeBytes(), remaining);
                    mapped.get(buf, 0, batch);
                    out.write(buf, 0, batch);
                    totalWritten.addAndGet(batch);
                    remaining -= batch;
                }
                out.flush();
            }

            log.info("[{}] Chunk {} done ({} MB)",
                    transferId, chunkIndex, length / (1024 * 1024));
            return null;    // Void — ShutdownOnFailure needs a return type

        } finally {
            closeQuietly(sftp);
            if (ssh != null) {
                sshPool.incrementUploadCount();
                sshPool.returnClient(ssh);
            }
        }
    }

    // ── Local NIO2 fallback (cfg.enabled() == false) ──────────────────────────

    private TransferResult transferLocal(String  sourceFile,
                                         String  destinationPath,
                                         String  destinationFile,
                                         String  separator,
                                         String  transferId,
                                         Instant start) {
        try {
            Path src  = Path.of(sourceFile);
            Path dest = Path.of(destinationPath + separator + destinationFile);
            Files.createDirectories(dest.getParent());
            long     written = Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
            Duration elapsed = Duration.between(start, Instant.now());
            return new TransferResult.Success(
                    written, elapsed, toMBps(written, elapsed), transferId);
        } catch (Exception ex) {
            log.error("[{}] Local copy failed", transferId, ex);
            return new TransferResult.Failure("Local copy error", ex, transferId);
        }
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    private static double toMBps(long bytes, Duration elapsed) {
        return (bytes / (1024.0 * 1024.0)) / Math.max(elapsed.toMillis() / 1000.0, 0.001);
    }

    private void closeQuietly(Closeable c) {
        if (c != null) try { c.close(); } catch (IOException ignored) {}
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        log.info("SftpFileCopy shutdown complete");
    }
}


package com.enterprise.sftp;

import com.enterprise.sftp.config.SftpTransferProperties;
import com.enterprise.sftp.config.SftpConnectionConfig.PooledSshClient;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SFTP file copy.
 */
@Component
public class SftpFileCopy {

    private static final Logger log = LoggerFactory.getLogger(SftpFileCopy.class);

    private final SftpTransferProperties cfg;
    private final PooledSshClient        sshPool;

    /**
     * Java 17: fixed platform-thread pool.
     *
     * <p>Size is pinned to {@code sshPoolSize} — if we allowed more threads
     * than SSH clients, extra threads would immediately block on
     * {@link PooledSshClient#borrowClient()}, wasting OS resources.
     * Java 21 virtual threads park cheaply, so no such coupling is needed there.
     */
    private final ExecutorService transferExecutor;

    public SftpFileCopy(SftpTransferProperties cfg, PooledSshClient sshPool) {
        this.cfg             = cfg;
        this.sshPool         = sshPool;
        this.transferExecutor = Executors.newFixedThreadPool(cfg.sshPoolSize());
        log.info("SftpFileCopy [Java 17] ready — sftp={} chunkSize={}MB poolSize={}",
                cfg.enabled(), cfg.chunkSizeBytes() / (1024 * 1024), cfg.sshPoolSize());
    }

    // ── Sealed result hierarchy (GA since Java 17) ────────────────────────────

    public sealed interface TransferResult
            permits TransferResult.Success, TransferResult.Failure {

        record Success(long     bytesTransferred,
                       Duration elapsed,
                       double   throughputMBps,
                       String   transferId) implements TransferResult {}

        record Failure(String    reason,
                       Throwable cause,
                       String    transferId) implements TransferResult {}
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public TransferResult copyFile(String sourceFile,
                                   String destinationPath,
                                   String destinationFile) {

        final String  transferId = UUID.randomUUID().toString();
        final Instant start      = Instant.now();
        final String  separator  = StringUtils.endsWith(destinationPath, File.separator)
                ? "" : File.separator;

        log.info("[{}] Transfer start — src={} dst={}/{}",
                transferId, sourceFile, destinationPath, destinationFile);

        return cfg.enabled()
                ? transferViaSftp(sourceFile, destinationPath, destinationFile,
                                  separator, transferId, start)
                : transferLocal(sourceFile, destinationPath, destinationFile,
                                separator, transferId, start);
    }

    // ── SFTP dispatch ─────────────────────────────────────────────────────────

    private TransferResult transferViaSftp(String sourceFile,
                                           String destinationPath,
                                           String destinationFile,
                                           String separator,
                                           String transferId,
                                           Instant start) {
        Path sourcePath = Path.of(sourceFile);
        long fileSize;
        try {
            fileSize = Files.size(sourcePath);
        } catch (IOException ex) {
            return new TransferResult.Failure("Cannot read source file size", ex, transferId);
        }

        String remoteDest = destinationPath + separator + destinationFile;

        return cfg.requiresChunkedTransfer(fileSize)
                ? chunkedParallelTransfer(sourcePath, fileSize, remoteDest, transferId, start)
                : singleStreamTransfer(sourcePath, fileSize, remoteDest, transferId, start);
    }

    // ── Single-stream transfer ────────────────────────────────────────────────

    private TransferResult singleStreamTransfer(Path    sourcePath,
                                                long    fileSize,
                                                String  remoteDest,
                                                String  transferId,
                                                Instant start) {
        SSHClient  ssh  = null;
        SFTPClient sftp = null;
        try {
            ssh  = sshPool.borrowClient();
            sftp = ssh.newSFTPClient();

            try (RemoteFile remoteFile = sftp.open(
                         remoteDest, EnumSet.of(OpenMode.WRITE, OpenMode.CREAT));
                 InputStream  in  = new BufferedInputStream(
                         Files.newInputStream(sourcePath), cfg.bufferSizeBytes());
                 OutputStream out = new BufferedOutputStream(
                         remoteFile.new RemoteFileOutputStream(), cfg.bufferSizeBytes())) {

                in.transferTo(out);   // InputStream.transferTo() — GA since Java 9
                out.flush();
            }

            Duration elapsed = Duration.between(start, Instant.now());
            log.info("[{}] Single-stream complete — {:.2f} MB/s",
                    transferId, toMBps(fileSize, elapsed));
            return new TransferResult.Success(
                    fileSize, elapsed, toMBps(fileSize, elapsed), transferId);

        } catch (Exception ex) {
            log.error("[{}] Single-stream failed", transferId, ex);
            return new TransferResult.Failure("SFTP single-stream error", ex, transferId);
        } finally {
            closeQuietly(sftp);
            if (ssh != null) sshPool.returnClient(ssh);
        }
    }

    // ── Chunked parallel transfer — Java 17 CompletableFuture ────────────────

    /**
     * Parallel chunk upload using {@link CompletableFuture} on a fixed thread pool.
     *
     * <h4>Failure handling vs Java 21</h4>
     * <p>Java 21 {@code StructuredTaskScope.ShutdownOnFailure} cancels sibling
     * virtual threads the instant any chunk throws.  Here the equivalent is:
     * <ol>
     *   <li>{@code CompletableFuture.allOf(...).get(totalTimeout)} blocks until
     *       all futures complete <em>or</em> the deadline passes.</li>
     *   <li>If {@code get()} throws (any future completed exceptionally),
     *       {@code executor.shutdownNow()} interrupts all running chunk threads.</li>
     *   <li>A second pass checks each future for exceptions via
     *       {@link CompletableFuture#isCompletedExceptionally()}.</li>
     * </ol>
     *
     * <h4>Timeout</h4>
     * <p>A single wall-clock deadline — {@code timeoutPerChunkMinutes × chunkCount}
     * — is passed to {@code allOf.get(timeout, unit)}, preserving the same
     * single-deadline semantics as {@code scope.joinUntil(deadline)}.
     */
    private TransferResult chunkedParallelTransfer(Path    sourcePath,
                                                   long    fileSize,
                                                   String  remoteDest,
                                                   String  transferId,
                                                   Instant start) {
        int        totalChunks   = cfg.chunkCount(fileSize);
        long       totalTimeout  = cfg.timeoutPerChunkMinutes() * (long) totalChunks;
        AtomicLong totalWritten  = new AtomicLong();

        // ── Submit one task per chunk to the fixed platform-thread pool ───────
        List<CompletableFuture<Void>> futures = new ArrayList<>(totalChunks);

        for (int i = 0; i < totalChunks; i++) {
            final int  idx    = i;
            final long offset = (long) i * cfg.chunkSizeBytes();
            final long length = cfg.chunkLength(i, totalChunks, fileSize);

            CompletableFuture<Void> future = CompletableFuture.runAsync(
                    () -> {
                        try {
                            transferChunk(sourcePath, remoteDest, idx,
                                          offset, length, transferId, totalWritten);
                        } catch (Exception ex) {
                            // Wrap checked exception — runAsync requires Runnable
                            throw new CompletionException(ex);
                        }
                    },
                    transferExecutor);

            futures.add(future);
        }

        // ── Wait for all chunks under a single wall-clock deadline ────────────
        CompletableFuture<Void> allChunks =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allChunks.get(totalTimeout, TimeUnit.MINUTES);

        } catch (TimeoutException ex) {
            // Cancel remaining — best-effort interrupt of running platform threads
            futures.forEach(f -> f.cancel(true));
            log.error("[{}] Transfer deadline exceeded ({}m × {} chunks)",
                    transferId, cfg.timeoutPerChunkMinutes(), totalChunks);
            return new TransferResult.Failure("Transfer deadline exceeded", ex, transferId);

        } catch (InterruptedException ex) {
            futures.forEach(f -> f.cancel(true));
            Thread.currentThread().interrupt();
            return new TransferResult.Failure("Transfer interrupted", ex, transferId);

        } catch (ExecutionException ex) {
            // One or more chunks failed — cancel the rest
            futures.forEach(f -> f.cancel(true));
            log.error("[{}] Chunk failed — remaining chunks cancelled",
                    transferId, ex.getCause());
            return new TransferResult.Failure(
                    "Chunk transfer failed", ex.getCause(), transferId);
        }

        // ── Verify no silent failures (non-exception path) ───────────────────
        boolean anyFailed = futures.stream().anyMatch(CompletableFuture::isCompletedExceptionally);
        if (anyFailed) {
            log.warn("[{}] One or more chunks reported failure post-join", transferId);
            return new TransferResult.Failure(
                    "One or more chunks failed", null, transferId);
        }

        Duration elapsed = Duration.between(start, Instant.now());
        double   mbps    = toMBps(fileSize, elapsed);
        log.info("[{}] Chunked complete — {:.2f} MB/s over {} chunks",
                transferId, mbps, totalChunks);
        return new TransferResult.Success(fileSize, elapsed, mbps, transferId);
    }

    // ── One chunk — runs on a platform thread ─────────────────────────────────

    /**
     * Executes on a fixed platform thread from {@link #transferExecutor}.
     *
     * <p><b>Java 17 note:</b> platform threads block on every SFTP I/O call,
     * holding their OS thread for the duration.  This is why the pool size
     * must equal the SSH pool size — there is no lightweight parking.
     * Java 21 virtual threads park the carrier thread during I/O, making
     * large chunk counts far cheaper.
     *
     * @throws Exception re-thrown so {@link CompletableFuture} captures the failure
     */
    private void transferChunk(Path       sourcePath,
                                String     remoteDest,
                                int        chunkIndex,
                                long       offset,
                                long       length,
                                String     transferId,
                                AtomicLong totalWritten) throws Exception {
        SSHClient  ssh  = null;
        SFTPClient sftp = null;
        try {
            ssh  = sshPool.borrowClient();
            sftp = ssh.newSFTPClient();

            try (FileChannel  channel = FileChannel.open(sourcePath, StandardOpenOption.READ);
                 RemoteFile   remote  = sftp.open(
                         remoteDest,
                         EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.APPEND));
                 OutputStream out     = new BufferedOutputStream(
                         remote.new RemoteFileOutputStream(offset), cfg.bufferSizeBytes())) {

                java.nio.MappedByteBuffer mapped =
                        channel.map(FileChannel.MapMode.READ_ONLY, offset, length);

                byte[] buf       = new byte[cfg.bufferSizeBytes()];
                long   remaining = length;

                while (remaining > 0) {
                    int batch = (int) Math.min(cfg.bufferSizeBytes(), remaining);
                    mapped.get(buf, 0, batch);
                    out.write(buf, 0, batch);
                    totalWritten.addAndGet(batch);
                    remaining -= batch;
                }
                out.flush();
            }

            log.info("[{}] Chunk {} done ({} MB)",
                    transferId, chunkIndex, length / (1024 * 1024));

        } finally {
            closeQuietly(sftp);
            if (ssh != null) {
                sshPool.incrementUploadCount();
                sshPool.returnClient(ssh);
            }
        }
    }

    // ── Local NIO2 fallback (cfg.enabled() == false) ──────────────────────────

    private TransferResult transferLocal(String  sourceFile,
                                         String  destinationPath,
                                         String  destinationFile,
                                         String  separator,
                                         String  transferId,
                                         Instant start) {
        try {
            Path src  = Path.of(sourceFile);
            Path dest = Path.of(destinationPath + separator + destinationFile);
            Files.createDirectories(dest.getParent());
            long     written = Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
            Duration elapsed = Duration.between(start, Instant.now());
            return new TransferResult.Success(
                    written, elapsed, toMBps(written, elapsed), transferId);
        } catch (Exception ex) {
            log.error("[{}] Local copy failed", transferId, ex);
            return new TransferResult.Failure("Local copy error", ex, transferId);
        }
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    private static double toMBps(long bytes, Duration elapsed) {
        return (bytes / (1024.0 * 1024.0)) / Math.max(elapsed.toMillis() / 1000.0, 0.001);
    }

    private void closeQuietly(Closeable c) {
        if (c != null) try { c.close(); } catch (IOException ignored) {}
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        log.info("SftpFileCopy [Java 17] shutdown — waiting {}m for in-flight transfers",
                cfg.executorShutdownTimeoutMinutes());
        transferExecutor.shutdown();
        try {
            if (!transferExecutor.awaitTermination(
                    cfg.executorShutdownTimeoutMinutes(), TimeUnit.MINUTES)) {
                transferExecutor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            transferExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}





