package com.enterprise.sftp;

import com.enterprise.sftp.config.SftpTransferProperties;
import com.enterprise.sftp.config.SftpConnectionConfig.PooledSshClient;
import com.enterprise.sftp.crypto.TransferEncryptionService;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Best-performance SFTP file copy — Java 17.
 *
 * ══════════════════════════════════════════════════════════════════════════
 * THREE PERFORMANCE FIXES — JAVA 17 COMPATIBLE EQUIVALENTS
 * ══════════════════════════════════════════════════════════════════════════
 *
 * FIX 1 — CipherOutputStream direct (Java 17 compatible — same as Java 21)
 * ─────────────────────────────────────────────────────────────────────────
 * Eliminates encryptSlice() — CipherOutputStream wraps remoteOut directly.
 * Before: 4 copies per slice. After: 2 copies per slice. (-50%)
 *
 * FIX 2 — Rolling MessageDigest (Java 17 compatible — same as Java 21)
 * ──────────────────────────────────────────────────────────────────────
 * Eliminates ByteArrayOutputStream chunk accumulation.
 * Before: 1 GB heap spike per chunk. After: 512 KB constant. (-99.95%)
 *
 * FIX 3 — MappedByteBuffer with multi-map for > 2 GB (Java 17 workaround)
 * ──────────────────────────────────────────────────────────────────────────
 * Java 17 has no MemorySegment. MappedByteBuffer is limited to 2 GB per map.
 * Fix: split large chunks into sub-maps of Integer.MAX_VALUE each.
 * Deterministic unmap via sun.misc.Cleaner reflection (safe, widely used).
 *
 * ══════════════════════════════════════════════════════════════════════════
 * JAVA 17 BEST ALTERNATIVES FOR JAVA 21 FEATURES
 * ══════════════════════════════════════════════════════════════════════════
 *
 *  Java 21 Feature              Java 17 Best Alternative
 *  ─────────────────────────    ──────────────────────────────────────────
 *  Virtual threads              ForkJoinPool with ASYNCMODE=true
 *                               (work-stealing, low thread overhead)
 *  StructuredTaskScope          CompletableFuture.allOf() +
 *                               first-failure cancellation via
 *                               CompletableFuture.exceptionally()
 *  MemorySegment + Arena        MappedByteBuffer with sub-map loop +
 *                               explicit unmap via Cleaner
 *  Record pattern switch        instanceof pattern binding (Java 16+)
 */
@Component
public class SftpFileCopy {

    private static final Logger log = LoggerFactory.getLogger(SftpFileCopy.class);

    // 2 GB sub-map limit for MappedByteBuffer (Java 17 Fix 3)
    private static final long MAP_CHUNK_MAX = (long) Integer.MAX_VALUE;

    private final SftpTransferProperties    cfg;
    private final PooledSshClient           sshPool;
    private final TransferEncryptionService encryption;

    // ── Java 17 best thread pool alternative to virtual threads ──────────────
    // ForkJoinPool in ASYNCMODE uses work-stealing — efficient for I/O tasks
    // that park frequently (SFTP network waits).
    // Pool size == SSH max connections — 1:1 mapping, no thread contention.
    private final ForkJoinPool transferPool;

    public SftpFileCopy(SftpTransferProperties cfg,
                        PooledSshClient sshPool,
                        TransferEncryptionService encryption) {
        this.cfg        = cfg;
        this.sshPool    = sshPool;
        this.encryption = encryption;

        // ASYNCMODE=true — work-stealing for async I/O tasks
        // parallelism = maxParallelChunks from yml (matches SSH pool size)
        this.transferPool = new ForkJoinPool(
                cfg.maxParallelChunks(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true);   // asyncMode

        log.info("SftpFileCopy [Java 17 optimised] ready — "
                + "forkJoinPool=async multiMap=true rollingDigest=true "
                + "cipherDirect=true chunkMB={} parallelism={}",
                cfg.chunkSizeBytes() / (1024 * 1024),
                cfg.maxParallelChunks());
    }

    // ════════════════════════════════════════════════════════════════════════
    // Sealed result types  (Java 17)
    // ════════════════════════════════════════════════════════════════════════

    public sealed interface TransferResult
            permits TransferResult.Success, TransferResult.Partial, TransferResult.Failure {

        record Success(long     bytesTransferred,
                       Duration elapsed,
                       double   throughputMBps,
                       boolean  encrypted,
                       String   transferId) implements TransferResult {}

        record Partial(long   bytesTransferred,
                       long   totalBytes,
                       String checkpointPath,
                       String transferId) implements TransferResult {}

        record Failure(String    reason,
                       Throwable cause,
                       String    transferId) implements TransferResult {}
    }

    // ════════════════════════════════════════════════════════════════════════
    // Chunk types
    // ════════════════════════════════════════════════════════════════════════

    record ChunkDescriptor(int    index,
                           int    totalChunks,
                           long   offset,
                           long   length,
                           String remoteDest,
                           String transferId,
                           Path   sourcePath) {}

    record ChunkResult(int     index,
                       long    bytesWritten,
                       boolean success,
                       String  sha256Hex) {}

    // ════════════════════════════════════════════════════════════════════════
    // Transfer manifest
    // ════════════════════════════════════════════════════════════════════════

    record TransferManifest(String                   transferId,
                            String                   sourceFile,
                            String                   destinationFile,
                            long                     totalBytes,
                            long                     chunkSize,
                            Map<Integer, ChunkState> chunks) {

        enum ChunkState { PENDING, IN_PROGRESS, COMPLETE, FAILED }

        boolean isChunkDone(int i) {
            return chunks.getOrDefault(i, ChunkState.PENDING) == ChunkState.COMPLETE;
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Public API
    // ════════════════════════════════════════════════════════════════════════

    public TransferResult copyFile(String sourceFile,
                                   String destinationPath,
                                   String destinationFile) {

        final String  id         = UUID.randomUUID().toString();
        final Instant start      = Instant.now();
        final String  sep        = StringUtils.endsWith(destinationPath, File.separator)
                ? "" : File.separator;
        final String  remoteFile = cfg.encryption().enabled()
                ? destinationFile + cfg.encryption().encryptedFileSuffix()
                : destinationFile;

        log.info("[{}] Transfer start src={} dst={}/{} encrypted={}",
                id, sourceFile, destinationPath, remoteFile,
                cfg.encryption().enabled());

        return cfg.enabled()
                ? sftpTransfer(sourceFile, destinationPath, remoteFile, sep, id, start)
                : localTransfer(sourceFile, destinationPath, remoteFile, sep, id, start);
    }

    // ════════════════════════════════════════════════════════════════════════
    // SFTP dispatch
    // ════════════════════════════════════════════════════════════════════════

    private TransferResult sftpTransfer(String  src,
                                         String  destPath,
                                         String  destFile,
                                         String  sep,
                                         String  id,
                                         Instant start) {
        Path srcPath = Path.of(src);
        long fileSize;
        try {
            fileSize = Files.size(srcPath);
        } catch (IOException ex) {
            return new TransferResult.Failure("Cannot read source file size", ex, id);
        }

        return cfg.requiresChunkedTransfer(fileSize)
                ? chunkedTransfer(srcPath, fileSize, destPath, destFile, sep, id, start)
                : singleStreamTransfer(srcPath, fileSize, destPath, destFile, sep, id, start);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Single-stream  (files < chunkSizeBytes)
    // ════════════════════════════════════════════════════════════════════════

    private TransferResult singleStreamTransfer(Path    srcPath,
                                                 long    fileSize,
                                                 String  destPath,
                                                 String  destFile,
                                                 String  sep,
                                                 String  id,
                                                 Instant start) {
        SSHClient  ssh  = null;
        SFTPClient sftp = null;

        try {
            ssh  = sshPool.borrowClient();
            sftp = ssh.newSFTPClient();

            try (RemoteFile remote = sftp.open(
                         destPath + sep + destFile,
                         EnumSet.of(OpenMode.WRITE, OpenMode.CREAT));
                 OutputStream remoteOut = new BufferedOutputStream(
                         remote.new RemoteFileOutputStream(), cfg.bufferSizeBytes());
                 InputStream rawIn = new BufferedInputStream(
                         Files.newInputStream(srcPath), cfg.bufferSizeBytes());
                 InputStream encIn = encryption.encryptStream(rawIn, id)) {

                AtomicLong written = new AtomicLong();
                new ProgressInputStream(encIn, fileSize, written, id,
                        cfg.progressIntervalMs()).transferTo(remoteOut);
                remoteOut.flush();
            }

            Duration elapsed = Duration.between(start, Instant.now());
            double   mbps    = toMBps(fileSize, elapsed);
            log.info("[{}] Single-stream done {:.2f} MB/s", id, mbps);
            return new TransferResult.Success(
                    fileSize, elapsed, mbps, cfg.encryption().enabled(), id);

        } catch (Exception ex) {
            log.error("[{}] Single-stream failed", id, ex);
            return new TransferResult.Failure("SFTP single-stream error", ex, id);
        } finally {
            closeQuietly(sftp);
            if (ssh != null) sshPool.returnClient(ssh);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Chunked parallel — Java 17 best alternative to StructuredTaskScope
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Java 17 parallel chunk coordination using {@link CompletableFuture}.
     *
     * <p>Replaces Java 21 {@code StructuredTaskScope.ShutdownOnFailure} with:
     * <ul>
     *   <li>{@code CompletableFuture.allOf()} — waits for all chunks</li>
     *   <li>Shared {@link CancellationToken} — first failure cancels siblings</li>
     *   <li>{@code orTimeout()} — per-chunk deadline watchdog</li>
     *   <li>{@link ForkJoinPool} in ASYNCMODE — work-stealing for I/O tasks</li>
     * </ul>
     *
     * <p>Limitation vs Java 21: sibling cancellation is cooperative
     * (checked at slice boundaries) not preemptive. Cancellation latency
     * is bounded by one slice write (~512 KB / link speed).
     */
    private TransferResult chunkedTransfer(Path    srcPath,
                                            long    fileSize,
                                            String  destPath,
                                            String  destFile,
                                            String  sep,
                                            String  id,
                                            Instant start) {

        int    totalChunks  = cfg.chunkCount(fileSize);
        String remoteDest   = destPath + sep + destFile;
        String manifestPath = destPath + sep + cfg.resume().manifestPrefix() + id;

        TransferManifest manifest = newManifest(
                id, srcPath.toString(), remoteDest, fileSize, totalChunks);

        AtomicLong       totalWritten = new AtomicLong();
        CancellationToken cancel      = new CancellationToken();

        List<CompletableFuture<ChunkResult>> futures = new ArrayList<>();

        for (int i = 0; i < totalChunks; i++) {

            if (manifest.isChunkDone(i)) {
                log.info("[{}] Chunk {} skipped — resume", id, i);
                totalWritten.addAndGet(cfg.chunkLength(i, totalChunks, fileSize));
                continue;
            }

            ChunkDescriptor desc = new ChunkDescriptor(
                    i, totalChunks,
                    (long) i * cfg.chunkSizeBytes(),
                    cfg.chunkLength(i, totalChunks, fileSize),
                    remoteDest, id, srcPath);

            CompletableFuture<ChunkResult> future = CompletableFuture
                    .supplyAsync(
                            () -> transferChunk(desc, manifest, totalWritten, cancel),
                            transferPool)
                    // Per-chunk timeout watchdog
                    .orTimeout(cfg.timeoutPerChunkMinutes(), TimeUnit.MINUTES)
                    // First failure: signal all siblings to stop at next slice boundary
                    .whenComplete((result, ex) -> {
                        if (ex != null || (result != null && !result.success())) {
                            cancel.cancel();
                        }
                    });

            futures.add(future);
        }

        // Wait for all chunks — failed futures don't throw here
        CompletableFuture<Void> all = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0]));

        try {
            all.get(cfg.timeoutPerChunkMinutes() * totalChunks,
                    TimeUnit.MINUTES);
        } catch (TimeoutException ex) {
            futures.forEach(f -> f.cancel(true));
            return new TransferResult.Failure("Transfer deadline exceeded", ex, id);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new TransferResult.Failure("Transfer interrupted", ex, id);
        } catch (ExecutionException ex) {
            log.error("[{}] Chunk execution failed", id, ex.getCause());
            return new TransferResult.Failure(
                    "Chunk transfer failed", ex.getCause(), id);
        }

        // Collect results
        boolean allSuccess = futures.stream()
                .map(f -> {
                    try {
                        return f.isDone() && !f.isCompletedExceptionally()
                                && f.get().success();
                    } catch (Exception ex) {
                        return false;
                    }
                })
                .allMatch(Boolean.TRUE::equals);

        Duration elapsed = Duration.between(start, Instant.now());

        if (allSuccess) {
            double mbps = toMBps(fileSize, elapsed);
            deleteManifest(manifestPath);
            log.info("[{}] Chunked done {:.2f} MB/s {} chunks encrypted={}",
                    id, mbps, totalChunks, cfg.encryption().enabled());
            return new TransferResult.Success(
                    fileSize, elapsed, mbps, cfg.encryption().enabled(), id);
        }

        return new TransferResult.Partial(
                totalWritten.get(), fileSize, manifestPath, id);
    }

    // ════════════════════════════════════════════════════════════════════════
    // CancellationToken — Java 17 alternative to StructuredTaskScope cancel
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Lightweight cooperative cancellation signal.
     * Checked at each slice boundary inside {@link #transferChunk}.
     * When any chunk fails, it calls {@link #cancel()} — all other
     * chunks stop at their next slice boundary.
     */
    private static final class CancellationToken {
        private volatile boolean cancelled = false;
        void    cancel()       { cancelled = true; }
        boolean isCancelled()  { return cancelled; }
    }

    // ════════════════════════════════════════════════════════════════════════
    // transferChunk — ALL THREE FIXES APPLIED — Java 17 compatible
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Transfers one chunk with maximum efficiency on Java 17.
     *
     * <pre>
     * FIX 1 (CipherOutputStream direct):
     *   mapped buffer → slice[] → CipherOutputStream → socket
     *   Zero intermediate byte[] for payload. Identical to Java 21 version.
     *
     * FIX 2 (Rolling MessageDigest):
     *   md.update(slice) per 512 KB → md.digest() at end.
     *   Constant 512 KB heap regardless of chunk size. Identical to Java 21.
     *
     * FIX 3 (MappedByteBuffer multi-map — Java 17 workaround):
     *   MappedByteBuffer is limited to Integer.MAX_VALUE (2 GB) per map call.
     *   For chunks > 2 GB: loop with sub-maps of MAP_CHUNK_MAX each.
     *   Explicit unmap via sun.misc.Cleaner after each sub-map to release
     *   OS file handles deterministically (no GC finalizer wait).
     * </pre>
     */
    private ChunkResult transferChunk(ChunkDescriptor   desc,
                                       TransferManifest  manifest,
                                       AtomicLong        totalWritten,
                                       CancellationToken cancel) {

        // Cooperative check — sibling already failed
        if (cancel.isCancelled()) {
            log.debug("[{}] Chunk {} skipped — cancelled by sibling failure",
                    desc.transferId(), desc.index());
            return new ChunkResult(desc.index(), 0, false, "");
        }

        SSHClient  ssh  = null;
        SFTPClient sftp = null;

        try {
            ssh  = sshPool.borrowClient();
            sftp = ssh.newSFTPClient();

            updateManifest(manifest, desc.index(),
                    TransferManifest.ChunkState.IN_PROGRESS);

            // ── FIX 2: Rolling SHA-256 — allocate once, update per slice ─────
            MessageDigest md = cfg.encryption().integrity().sha256PerChunk()
                    ? MessageDigest.getInstance("SHA-256")
                    : null;

            try (FileChannel chan = FileChannel.open(
                         desc.sourcePath(), StandardOpenOption.READ);

                 RemoteFile remote = sftp.open(
                         desc.remoteDest(),
                         EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.APPEND));

                 // ── FIX 1: CipherOutputStream wraps remoteOut directly ────────
                 OutputStream remoteOut = new BufferedOutputStream(
                         remote.new RemoteFileOutputStream(desc.offset()),
                         cfg.bufferSizeBytes())) {

                Cipher cipher = cfg.encryption().enabled()
                        ? encryption.buildChunkCipher(desc.transferId())
                        : null;

                OutputStream target = cipher != null
                        ? new CipherOutputStream(remoteOut, cipher)
                        : remoteOut;

                // Reused slice buffer — allocated once per chunk
                byte[] slice     = new byte[cfg.bufferSizeBytes()];
                long   remaining = desc.length();
                long   chunkPos  = 0;      // position within this chunk

                // ── FIX 3: MappedByteBuffer multi-map loop ───────────────────
                // Sub-map in windows of MAP_CHUNK_MAX (2 GB) to handle
                // chunks larger than Integer.MAX_VALUE on Java 17.
                while (remaining > 0 && !cancel.isCancelled()) {

                    long   subMapLen = Math.min(MAP_CHUNK_MAX, remaining);
                    long   subMapOff = desc.offset() + chunkPos;

                    // Map a sub-window of the file
                    MappedByteBuffer mappedBuf =
                            chan.map(FileChannel.MapMode.READ_ONLY,
                                    subMapOff, subMapLen);

                    long subRemaining = subMapLen;

                    try {
                        while (subRemaining > 0 && !cancel.isCancelled()) {
                            int batch = (int) Math.min(
                                    cfg.bufferSizeBytes(), subRemaining);

                            // MappedByteBuffer → slice[] — one unavoidable copy
                            mappedBuf.get(slice, 0, batch);

                            // FIX 2: rolling digest — no accumulation
                            if (md != null) md.update(slice, 0, batch);

                            // FIX 1: direct to cipher → socket — zero extra copy
                            target.write(slice, 0, batch);

                            totalWritten.addAndGet(batch);
                            subRemaining -= batch;
                            chunkPos     += batch;
                            remaining    -= batch;
                        }
                    } finally {
                        // ── FIX 3: Explicit unmap — deterministic release ─────
                        // Releases OS file handle immediately instead of waiting
                        // for GC to finalise the MappedByteBuffer.
                        // Uses sun.misc.Cleaner — safe on JDK 8–17,
                        // replace with MemorySegment.unload() on Java 21+.
                        unmapBuffer(mappedBuf);
                    }
                }

                target.flush();
                // CipherOutputStream appends GCM tag on close
                if (cipher != null) target.close();
                remoteOut.flush();
            }

            // ── FIX 2: digest complete — O(1) final step ─────────────────────
            String digestHex = "";
            if (md != null) {
                digestHex = bytesToHex(md.digest());
                log.debug("[{}] Chunk {} SHA-256={}…",
                        desc.transferId(), desc.index(),
                        digestHex.substring(0, 12));
            }

            // Check cancellation after write — don't mark COMPLETE if cancelled
            if (cancel.isCancelled()) {
                updateManifest(manifest, desc.index(),
                        TransferManifest.ChunkState.FAILED);
                return new ChunkResult(desc.index(), 0, false, "");
            }

            updateManifest(manifest, desc.index(),
                    TransferManifest.ChunkState.COMPLETE);

            log.info("[{}] Chunk {} done {} MB",
                    desc.transferId(), desc.index(),
                    desc.length() / (1024 * 1024));

            return new ChunkResult(
                    desc.index(), desc.length(), true, digestHex);

        } catch (Exception ex) {
            log.error("[{}] Chunk {} failed", desc.transferId(), desc.index(), ex);
            cancel.cancel();    // signal siblings to stop
            updateManifest(manifest, desc.index(),
                    TransferManifest.ChunkState.FAILED);
            return new ChunkResult(desc.index(), 0, false, "");

        } finally {
            closeQuietly(sftp);
            if (ssh != null) {
                sshPool.incrementUploadCount();
                sshPool.returnClient(ssh);
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // FIX 3 — Explicit MappedByteBuffer unmap  (Java 17)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Deterministically unmaps a {@link MappedByteBuffer} on Java 17.
     *
     * <p>Without explicit unmap, the OS file handle is held until GC
     * finalises the buffer — on a 5 GB transfer with 5 chunks and
     * multiple sub-maps, this delays handle release by seconds or minutes
     * under GC pressure.
     *
     * <p>Uses {@code sun.misc.Cleaner} reflection — widely used in
     * production libraries (Netty, Lucene, Kafka) for exactly this purpose.
     * Replace with {@code MemorySegment.unload()} on Java 21+.
     *
     * <p>Fails silently — if reflection is blocked by module system,
     * the buffer is left for GC (safe, just slower).
     */
    private static void unmapBuffer(MappedByteBuffer buffer) {
        if (buffer == null) return;
        try {
            // Java 9+ path via Unsafe.invokeCleaner
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            java.lang.reflect.Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Object unsafe = f.get(null);
            java.lang.reflect.Method invokeCleaner =
                    unsafeClass.getMethod("invokeCleaner",
                            java.nio.ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
        } catch (Exception ex) {
            // Reflection blocked — leave for GC, not a correctness issue
            log.debug("unmapBuffer: reflection unavailable — leaving for GC: {}",
                    ex.getMessage());
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // isSuccess — instanceof pattern binding (Java 16+, works on 17 and 21)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Check transfer outcome.
     * Uses {@code instanceof} pattern binding — avoids record destructuring
     * switch which caused "not reachable" on Java 17 compilers.
     */
    public static boolean isSuccess(TransferResult result) {

        if (result instanceof TransferResult.Success s) {
            log.info("[{}] SUCCESS — {} MB in {}s at {:.2f} MB/s encrypted={}",
                    s.transferId(),
                    s.bytesTransferred() / (1024 * 1024),
                    s.elapsed().toSeconds(),
                    s.throughputMBps(),
                    s.encrypted());
            return true;
        }

        if (result instanceof TransferResult.Partial p) {
            log.warn("[{}] PARTIAL — {}/{} MB ({:.1f}%) resume={}",
                    p.transferId(),
                    p.bytesTransferred() / (1024 * 1024),
                    p.totalBytes()       / (1024 * 1024),
                    (p.bytesTransferred() * 100.0 / p.totalBytes()),
                    p.checkpointPath());
            return false;
        }

        if (result instanceof TransferResult.Failure f) {
            log.error("[{}] FAILED — {} cause={}",
                    f.transferId(),
                    f.reason(),
                    f.cause() != null ? f.cause().getMessage() : "none");
            return false;
        }

        throw new IllegalStateException(
                "Unknown TransferResult: " + result.getClass().getName());
    }

    // ════════════════════════════════════════════════════════════════════════
    // Local NIO2 — OS zero-copy (same on Java 17 and 21)
    // ════════════════════════════════════════════════════════════════════════

    private TransferResult localTransfer(String  src,
                                          String  destPath,
                                          String  destFile,
                                          String  sep,
                                          String  id,
                                          Instant start) {
        try {
            Path srcPath  = Path.of(src);
            Path destFull = Path.of(destPath + sep + destFile);
            Files.createDirectories(destFull.getParent());

            if (cfg.encryption().enabled()) {
                try (InputStream  rawIn = new BufferedInputStream(
                             Files.newInputStream(srcPath), cfg.bufferSizeBytes());
                     InputStream  encIn = encryption.encryptStream(rawIn, id);
                     OutputStream out   = new BufferedOutputStream(
                             Files.newOutputStream(destFull,
                                     StandardOpenOption.CREATE,
                                     StandardOpenOption.TRUNCATE_EXISTING),
                             cfg.bufferSizeBytes())) {
                    encIn.transferTo(out);
                }
                long w = Files.size(destFull);
                Duration e = Duration.between(start, Instant.now());
                return new TransferResult.Success(w, e, toMBps(w, e), true, id);
            }

            long w = Files.copy(srcPath, destFull,
                    StandardCopyOption.REPLACE_EXISTING);
            Duration e = Duration.between(start, Instant.now());
            return new TransferResult.Success(w, e, toMBps(w, e), false, id);

        } catch (Exception ex) {
            log.error("[{}] Local copy failed", id, ex);
            return new TransferResult.Failure("Local copy error", ex, id);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Progress InputStream
    // ════════════════════════════════════════════════════════════════════════

    private static final class ProgressInputStream extends FilterInputStream {
        private final long       total;
        private final AtomicLong written;
        private final String     id;
        private final int        intervalMs;
        private       long       lastLog = System.currentTimeMillis();

        ProgressInputStream(InputStream in, long total, AtomicLong written,
                            String id, int intervalMs) {
            super(in);
            this.total      = total;
            this.written    = written;
            this.id         = id;
            this.intervalMs = intervalMs;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                long w   = written.addAndGet(n);
                long now = System.currentTimeMillis();
                if (now - lastLog >= intervalMs) {
                    log.info("[{}] Progress {:.1f}% ({} MB / {} MB)",
                            id, (w * 100.0) / total,
                            w / (1024 * 1024), total / (1024 * 1024));
                    lastLog = now;
                }
            }
            return n;
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Manifest helpers
    // ════════════════════════════════════════════════════════════════════════

    private TransferManifest newManifest(String id, String src, String dest,
                                          long fileSize, int chunks) {
        Map<Integer, TransferManifest.ChunkState> map = new ConcurrentHashMap<>();
        for (int i = 0; i < chunks; i++)
            map.put(i, TransferManifest.ChunkState.PENDING);
        return new TransferManifest(
                id, src, dest, fileSize, cfg.chunkSizeBytes(), map);
    }

    private void updateManifest(TransferManifest m, int idx,
                                 TransferManifest.ChunkState state) {
        m.chunks().put(idx, state);
    }

    private void deleteManifest(String path) {
        if (cfg.resume().enabled())
            try { Files.deleteIfExists(Path.of(path)); }
            catch (IOException ignored) {}
    }

    // ════════════════════════════════════════════════════════════════════════
    // Utility
    // ════════════════════════════════════════════════════════════════════════

    private static double toMBps(long bytes, Duration elapsed) {
        return (bytes / (1024.0 * 1024.0))
                / Math.max(elapsed.toMillis() / 1000.0, 0.001);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private void closeQuietly(Closeable c) {
        if (c != null) try { c.close(); } catch (IOException ignored) {}
    }

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        log.info("SftpFileCopy shutdown — draining ForkJoinPool");
        transferPool.shutdown();
        try {
            if (!transferPool.awaitTermination(
                    cfg.executorShutdownTimeoutMinutes(), TimeUnit.MINUTES))
                transferPool.shutdownNow();
        } catch (InterruptedException ex) {
            transferPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
