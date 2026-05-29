import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone Java 21 SFTP file copy — no framework dependencies.
 *
 * <p><b>Usage:</b>
 * <pre>
 *   java --enable-preview -cp ".:lib/*" SftpFileCopy \
 *       &lt;sourceFile&gt; &lt;destinationPath&gt; &lt;destinationFile&gt;
 * </pre>
 *
 * <p><b>SFTP credentials</b> — edit the CONFIG section below or pass as
 * environment variables:
 * <pre>
 *   SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD,
 *   SFTP_KEY_PATH  (private key; takes priority over password)
 * </pre>
 *
 * <p><b>Performance model:</b>
 * <ul>
 *   <li>Small files ({@code < CHUNK_SIZE_BYTES}) — single-stream via
 *       {@link InputStream#transferTo}, one SSH connection.</li>
 *   <li>Large files ({@code >= CHUNK_SIZE_BYTES}) — parallel chunks on
 *       Java 21 virtual threads, coordinated by
 *       {@link StructuredTaskScope.ShutdownOnFailure}.
 *       Memory-mapped {@link FileChannel} reads keep heap pressure low.
 *       One SSH connection per chunk, drawn from the pool.</li>
 * </ul>
 *
 * <p><b>Compile (Java 21):</b>
 * <pre>
 *   javac --enable-preview --release 21 -cp "lib/*" SftpFileCopy.java
 * </pre>
 *
 * <p>Requires on classpath: {@code sshj-*.jar} and its transitive deps
 * (slf4j-api, bcprov, eddsa).
 */
public class SftpFileCopy {

    // =========================================================================
    // CONFIG — override via environment variables or edit directly
    // =========================================================================

    private static final String  SFTP_HOST             = env("SFTP_HOST",            "sftp.example.com");
    private static final int     SFTP_PORT             = intEnv("SFTP_PORT",          22);
    private static final String  SFTP_USER             = env("SFTP_USER",             "sftpuser");
    private static final String  SFTP_PASSWORD         = env("SFTP_PASSWORD",         "");      // used if no key
    private static final String  SFTP_KEY_PATH         = env("SFTP_KEY_PATH",         "");      // e.g. ~/.ssh/id_rsa

    /** Number of pooled SSH connections. Also caps chunk parallelism. */
    private static final int     SSH_POOL_SIZE         = intEnv("SSH_POOL_SIZE",       4);

    /** Files larger than this are transferred in parallel chunks. Default 64 MB. */
    private static final long    CHUNK_SIZE_BYTES      = longEnv("CHUNK_SIZE_BYTES",   64L * 1024 * 1024);

    /** Read/write buffer size per stream/chunk. Default 256 KB. */
    private static final int     BUFFER_SIZE_BYTES     = intEnv("BUFFER_SIZE_BYTES",   256 * 1024);

    /** Per-chunk timeout multiplier (minutes). Total deadline = this × chunkCount. */
    private static final long    TIMEOUT_PER_CHUNK_MIN = longEnv("TIMEOUT_PER_CHUNK_MIN", 10L);

    /** SSH connect + auth timeout (milliseconds). */
    private static final int     CONNECT_TIMEOUT_MS    = intEnv("CONNECT_TIMEOUT_MS", 15_000);

    // =========================================================================
    // Entry point
    // =========================================================================

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: SftpFileCopy <sourceFile> <destinationPath> <destinationFile>");
            System.exit(1);
        }

        String sourceFile      = args[0];
        String destinationPath = args[1];
        String destinationFile = args[2];

        PooledSshClient pool = null;
        try {
            pool = new PooledSshClient(SSH_POOL_SIZE);

            TransferResult result = copyFile(sourceFile, destinationPath, destinationFile, pool);

            switch (result) {
                case TransferResult.Success(long bytes, Duration elapsed, double mbps, String id) ->
                    System.out.printf("[%s] SUCCESS — %,d bytes @ %.2f MB/s in %ds%n",
                            id, bytes, mbps, elapsed.toSeconds());

                case TransferResult.Failure(String reason, Throwable cause, String id) -> {
                    System.err.printf("[%s] FAILED — %s%n", id, reason);
                    if (cause != null) cause.printStackTrace(System.err);
                    System.exit(2);
                }
            }

        } catch (Exception ex) {
            System.err.println("Fatal: " + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(3);
        } finally {
            if (pool != null) pool.close();
        }
    }

    // =========================================================================
    // Sealed result hierarchy
    // =========================================================================

    sealed interface TransferResult
            permits TransferResult.Success, TransferResult.Failure {

        record Success(long     bytesTransferred,
                       Duration elapsed,
                       double   throughputMBps,
                       String   transferId) implements TransferResult {}

        record Failure(String    reason,
                       Throwable cause,
                       String    transferId) implements TransferResult {}
    }

    // =========================================================================
    // Chunk coordination records
    // =========================================================================

    /** Immutable descriptor — passed into each virtual-thread chunk task. */
    record ChunkDescriptor(int    index,
                           int    totalChunks,
                           long   offset,
                           long   length,
                           String remoteDest,
                           String transferId,
                           Path   sourcePath) {}

    record ChunkResult(int     chunkIndex,
                       long    bytesWritten,
                       boolean success) {}

    // =========================================================================
    // SSH Connection Pool
    // =========================================================================

    /**
     * Fixed-size pool of {@link SSHClient} connections backed by an
     * {@link ArrayBlockingQueue}.
     *
     * <ul>
     *   <li>{@link #borrowClient()} blocks the calling virtual thread until a
     *       connection is available — this is the <em>sole</em> concurrency
     *       limiter for parallel chunks. No semaphore or extra locking needed.</li>
     *   <li>Stale/closed connections are detected on borrow and replaced
     *       transparently so the pool self-heals after transient network faults.</li>
     *   <li>Broken connections returned via {@link #returnClient} are replaced
     *       inline to keep pool size stable.</li>
     * </ul>
     */
    static final class PooledSshClient implements Closeable {

        private final ArrayBlockingQueue<SSHClient> pool;
        private final AtomicLong uploadCount = new AtomicLong();

        PooledSshClient(int size) throws IOException {
            this.pool = new ArrayBlockingQueue<>(size);
            for (int i = 0; i < size; i++) {
                pool.offer(openConnection());
            }
            System.out.printf("[pool] Initialised %d SSH connections → %s:%d%n",
                    size, SFTP_HOST, SFTP_PORT);
        }

        /** Blocks (virtual thread parks) until a healthy connection is available. */
        SSHClient borrowClient() throws InterruptedException, IOException {
            SSHClient client = pool.take();
            if (!client.isConnected() || !client.isAuthenticated()) {
                System.out.println("[pool] Stale connection detected — replacing");
                closeQuietly(client);
                client = openConnection();
            }
            return client;
        }

        /** Returns a connection; replaces it if no longer healthy. */
        void returnClient(SSHClient client) {
            if (client.isConnected() && client.isAuthenticated()) {
                pool.offer(client);
            } else {
                try {
                    pool.offer(openConnection());
                } catch (IOException ex) {
                    System.err.printf("[pool] Could not replace broken connection: %s%n",
                            ex.getMessage());
                }
            }
        }

        void incrementUploadCount() { uploadCount.incrementAndGet(); }
        long uploadCount()          { return uploadCount.get(); }

        @Override
        public void close() {
            List<SSHClient> drained = new ArrayList<>();
            pool.drainTo(drained);
            drained.forEach(SftpFileCopy::closeQuietly);
            System.out.printf("[pool] Closed. Total chunks uploaded: %d%n", uploadCount.get());
        }

        // ── Connection factory ────────────────────────────────────────────────

        private static SSHClient openConnection() throws IOException {
            SSHClient ssh = new SSHClient();
            // Replace PromiscuousVerifier with OpenSSHKnownHosts in production:
            //   ssh.addHostKeyVerifier(new OpenSSHKnownHosts(new File("~/.ssh/known_hosts")));
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.setConnectTimeout(CONNECT_TIMEOUT_MS);
            ssh.connect(SFTP_HOST, SFTP_PORT);

            if (!SFTP_KEY_PATH.isEmpty()) {
                KeyProvider keys = ssh.loadKeys(SFTP_KEY_PATH);
                ssh.authPublickey(SFTP_USER, keys);
            } else {
                ssh.authPassword(SFTP_USER, SFTP_PASSWORD);
            }
            return ssh;
        }
    }

    // =========================================================================
    // Copy orchestration
    // =========================================================================

    static TransferResult copyFile(String          sourceFile,
                                   String          destinationPath,
                                   String          destinationFile,
                                   PooledSshClient pool) {

        final String  transferId = UUID.randomUUID().toString();
        final Instant start      = Instant.now();
        final String  sep        = destinationPath.endsWith("/")
                || destinationPath.endsWith(File.separator) ? "" : "/";

        System.out.printf("[%s] Transfer start — src=%s  dst=%s%s%s%n",
                transferId, sourceFile, destinationPath, sep, destinationFile);

        Path sourcePath = Path.of(sourceFile);
        long fileSize;
        try {
            fileSize = Files.size(sourcePath);
        } catch (IOException ex) {
            return new TransferResult.Failure("Cannot read source file size", ex, transferId);
        }

        String remoteDest = destinationPath + sep + destinationFile;

        return fileSize >= CHUNK_SIZE_BYTES
                ? chunkedParallelTransfer(sourcePath, fileSize, remoteDest, transferId, start, pool)
                : singleStreamTransfer(sourcePath, fileSize, remoteDest, transferId, start, pool);
    }

    // =========================================================================
    // Single-stream  (file < CHUNK_SIZE_BYTES)
    // =========================================================================

    private static TransferResult singleStreamTransfer(Path            sourcePath,
                                                        long            fileSize,
                                                        String          remoteDest,
                                                        String          transferId,
                                                        Instant         start,
                                                        PooledSshClient pool) {
        SSHClient  sshClient  = null;
        SFTPClient sftpClient = null;

        try {
            sshClient  = pool.borrowClient();
            sftpClient = sshClient.newSFTPClient();

            try (RemoteFile remoteOut = sftpClient.open(
                         remoteDest, EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.TRUNC));

                 InputStream  src = new BufferedInputStream(
                         Files.newInputStream(sourcePath), BUFFER_SIZE_BYTES);

                 OutputStream dst = new BufferedOutputStream(
                         remoteOut.new RemoteFileOutputStream(), BUFFER_SIZE_BYTES)) {

                src.transferTo(dst);    // Java 9+ — no manual byte[] loop
                dst.flush();
            }

            Duration elapsed = Duration.between(start, Instant.now());
            double   mbps    = toMBps(fileSize, elapsed);
            System.out.printf("[%s] Single-stream complete — %.2f MB/s%n", transferId, mbps);
            return new TransferResult.Success(fileSize, elapsed, mbps, transferId);

        } catch (Exception ex) {
            System.err.printf("[%s] Single-stream failed: %s%n", transferId, ex.getMessage());
            return new TransferResult.Failure("SFTP single-stream error", ex, transferId);

        } finally {
            closeQuietly(sftpClient);
            if (sshClient != null) pool.returnClient(sshClient);
        }
    }

    // =========================================================================
    // Chunked parallel  (file >= CHUNK_SIZE_BYTES)
    // =========================================================================

    /**
     * Splits the file into {@code N = ceil(fileSize / CHUNK_SIZE_BYTES)} chunks
     * and transfers each on its own Java 21 virtual thread.
     *
     * <ul>
     *   <li>All chunks forked simultaneously; SSH pool caps actual parallelism.</li>
     *   <li>{@link StructuredTaskScope.ShutdownOnFailure} cancels every sibling
     *       the moment any single chunk throws — no partial silent writes.</li>
     *   <li>Single wall-clock deadline:
     *       {@code TIMEOUT_PER_CHUNK_MIN × totalChunks} minutes.</li>
     *   <li>Memory-mapped reads: OS page cache serves data; heap holds only
     *       one {@code BUFFER_SIZE_BYTES} slice at a time per chunk.</li>
     * </ul>
     */
    private static TransferResult chunkedParallelTransfer(Path            sourcePath,
                                                           long            fileSize,
                                                           String          remoteDest,
                                                           String          transferId,
                                                           Instant         start,
                                                           PooledSshClient pool) {

        int     totalChunks  = (int) Math.ceil((double) fileSize / CHUNK_SIZE_BYTES);
        Instant deadline     = start.plus(Duration.ofMinutes(TIMEOUT_PER_CHUNK_MIN * totalChunks));
        AtomicLong totalWritten = new AtomicLong();

        System.out.printf("[%s] Chunked transfer — %d chunks, pool=%d, deadline=%dm%n",
                transferId, totalChunks, SSH_POOL_SIZE, TIMEOUT_PER_CHUNK_MIN * totalChunks);

        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {

            List<StructuredTaskScope.Subtask<ChunkResult>> subtasks = new ArrayList<>(totalChunks);

            for (int i = 0; i < totalChunks; i++) {
                long chunkOffset = (long) i * CHUNK_SIZE_BYTES;
                long chunkLength = Math.min(CHUNK_SIZE_BYTES, fileSize - chunkOffset);

                ChunkDescriptor desc = new ChunkDescriptor(
                        i, totalChunks, chunkOffset, chunkLength,
                        remoteDest, transferId, sourcePath);

                // fork() submits to a fresh virtual thread immediately
                subtasks.add(scope.fork(() -> transferChunk(desc, totalWritten, pool)));
            }

            scope.joinUntil(deadline);   // blocks calling (platform) thread
            scope.throwIfFailed();       // re-throws first chunk exception if any

            Duration elapsed = Duration.between(start, Instant.now());
            double   mbps    = toMBps(fileSize, elapsed);
            System.out.printf("[%s] Chunked complete — %.2f MB/s over %d chunks%n",
                    transferId, mbps, totalChunks);
            return new TransferResult.Success(fileSize, elapsed, mbps, transferId);

        } catch (ExecutionException ex) {
            System.err.printf("[%s] Chunk failed — all siblings cancelled: %s%n",
                    transferId, ex.getCause().getMessage());
            return new TransferResult.Failure("Chunk transfer failed", ex.getCause(), transferId);

        } catch (TimeoutException ex) {
            System.err.printf("[%s] Transfer deadline exceeded (%dm × %d chunks)%n",
                    transferId, TIMEOUT_PER_CHUNK_MIN, totalChunks);
            return new TransferResult.Failure("Transfer deadline exceeded", ex, transferId);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new TransferResult.Failure("Transfer interrupted", ex, transferId);
        }
    }

    // =========================================================================
    // One chunk — runs on a virtual thread
    // =========================================================================

    /**
     * Executes on a Java 21 virtual thread forked by the scope.
     *
     * <ul>
     *   <li>Virtual thread <em>parks</em> (not OS-blocks) on
     *       {@link ArrayBlockingQueue#take} waiting for a pool connection —
     *       the carrier thread is released during that wait.</li>
     *   <li>Virtual thread parks again on SFTP network I/O — carrier thread
     *       free during network latency, so hundreds of chunks can be
     *       in-flight with only a handful of OS threads.</li>
     *   <li>Throws on any failure; {@link StructuredTaskScope.ShutdownOnFailure}
     *       then cancels all sibling virtual threads immediately.</li>
     * </ul>
     */
    private static ChunkResult transferChunk(ChunkDescriptor desc,
                                             AtomicLong      totalWritten,
                                             PooledSshClient pool) throws Exception {
        SSHClient  sshClient  = null;
        SFTPClient sftpClient = null;

        try {
            sshClient  = pool.borrowClient();          // parks virtual thread if pool empty
            sftpClient = sshClient.newSFTPClient();

            try (FileChannel channel = FileChannel.open(desc.sourcePath(), StandardOpenOption.READ);

                 RemoteFile remoteFile = sftpClient.open(
                         desc.remoteDest(),
                         EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.APPEND));

                 OutputStream remoteOut = new BufferedOutputStream(
                         remoteFile.new RemoteFileOutputStream(desc.offset()),
                         BUFFER_SIZE_BYTES)) {

                // Memory-mapped read — OS page cache; heap holds only one slice
                MappedByteBuffer mapped = channel.map(
                        FileChannel.MapMode.READ_ONLY, desc.offset(), desc.length());

                byte[] slice     = new byte[BUFFER_SIZE_BYTES];
                long   remaining = desc.length();

                while (remaining > 0) {
                    int batch = (int) Math.min(BUFFER_SIZE_BYTES, remaining);
                    mapped.get(slice, 0, batch);
                    remoteOut.write(slice, 0, batch);
                    totalWritten.addAndGet(batch);
                    remaining -= batch;
                }

                remoteOut.flush();
            }

            System.out.printf("[%s] Chunk %d/%d done (%d MB)%n",
                    desc.transferId(), desc.index() + 1, desc.totalChunks(),
                    desc.length() / (1024 * 1024));

            return new ChunkResult(desc.index(), desc.length(), true);

        } catch (Exception ex) {
            System.err.printf("[%s] Chunk %d failed: %s%n",
                    desc.transferId(), desc.index(), ex.getMessage());
            throw ex;   // triggers ShutdownOnFailure → all siblings cancelled

        } finally {
            closeQuietly(sftpClient);
            if (sshClient != null) {
                pool.incrementUploadCount();
                pool.returnClient(sshClient);
            }
        }
    }

    // =========================================================================
    // Utility
    // =========================================================================

    private static double toMBps(long bytes, Duration elapsed) {
        return (bytes / (1024.0 * 1024.0)) / Math.max(elapsed.toMillis() / 1000.0, 0.001);
    }

    private static void closeQuietly(Closeable c) {
        if (c != null) try { c.close(); } catch (IOException ignored) {}
    }

    // ── Environment helpers ───────────────────────────────────────────────────

    private static String env(String key, String defaultVal) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultVal;
    }

    private static int intEnv(String key, int defaultVal) {
        try { return Integer.parseInt(System.getenv(key)); }
        catch (Exception ignored) { return defaultVal; }
    }

    private static long longEnv(String key, long defaultVal) {
        try { return Long.parseLong(System.getenv(key)); }
        catch (Exception ignored) { return defaultVal; }
    }
}
