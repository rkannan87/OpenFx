

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.persistence.EntityManager;

import lombok.extern.slf4j.Slf4j;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

@Slf4j
public class ShipfileBuilderImpl implements ShipfileBuilder {

    private static final String OUT_SUFFIX = ".out";
    private static final String TEMP_FILE = "temp.txt";
    private static final String HEADER_SUFFIX = ".header.txt";
    private static final String TRAILER_SUFFIX = ".trailer.txt";

    /** Windows may hold a transient lock (AV scan, lagging writer); Linux never hits this. */
    private static final int LOCK_RETRIES = 5;
    private static final long LOCK_RETRY_WAIT_MS = 200L;

    private DirStructureHelper dirHelper;
    private HeaderTrailer headertrailer;
    private ExecuteSystemCommand command;   // retained for other callers in this class
    private long currentJobNum;

    @Override
    public boolean build(Job job, Customer cust, EntityManager em) throws TucException {
        String outFileName;
        try {
            log.info("build ship file for " + cust.getCustCode());
            if (headertrailer != null) {
                headertrailer.build(job, cust);
            }

            long jobnum = job.getId();
            this.currentJobNum = jobnum;

            Path workDir = Paths.get(dirHelper.getAbsoluteFullfillDir(cust.getCustCode(), jobnum));

            outFileName = dirHelper.getAbsoluteFullfillCompletedDir()
                    + cust.getCustCode() + "."
                    + DateUtil.getDateString(new Date(), "yyyyMMdd") + "."
                    + jobnum + OUT_SUFFIX;

            assembleShipFile(workDir, jobnum, Paths.get(outFileName));
        }
        catch (Exception e) {
            log.error("ship file...", e);
            throw new TucException("ship file...", e);
        }

        // Generate companion .out.encPan.csv if PAN substitution was used
        PanDecryptService decryptService = PanDecryptServiceHolder.getInstance();
        boolean vaultEnabled = (decryptService != null) && decryptService.isEnabled();
        if (vaultEnabled) {
            generateEncPanCsv(cust.getCustCode(), outFileName);
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Ship file assembly — replaces the find/cat/mv shell pipeline.
    // Pure java.nio, so behaviour is identical on Linux and Windows and
    // does not depend on which ExecuteSystemCommand impl is wired in.
    // ------------------------------------------------------------------

    /**
     * Concatenates every *.out beneath {@code workDir} into a temp file, then emits
     * either header + temp + trailer, or the temp file alone, to {@code target}.
     *
     * @param workDir fulfill directory for this customer/job
     * @param jobnum  job id, used to locate the header/trailer files
     * @param target  absolute path of the completed ship file
     */
    private void assembleShipFile(Path workDir, long jobnum, Path target) throws IOException {

        Path temp = workDir.resolve(TEMP_FILE);

        // A crashed prior run can leave temp.txt behind, still handle-locked on Windows.
        retryOnLock("delete stale " + temp, () -> Files.deleteIfExists(temp));
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }

        List<Path> parts = collectOutFiles(workDir, temp, target);
        if (parts.isEmpty()) {
            log.warn("no " + OUT_SUFFIX + " files found under " + workDir + " for job " + jobnum);
        }

        concatTo(temp, parts, CREATE, APPEND);
        log.info("concatenated " + parts.size() + " file(s) into " + temp);

        if (headertrailer != null) {
            Path header = workDir.resolve(jobnum + HEADER_SUFFIX);
            Path trailer = workDir.resolve(jobnum + TRAILER_SUFFIX);
            concatTo(target, List.of(header, temp, trailer), CREATE, TRUNCATE_EXISTING);
            retryOnLock("delete " + temp, () -> Files.deleteIfExists(temp));
        }
        else {
            // ATOMIC_MOVE is deliberately not requested — it throws when the completed
            // directory sits on a different mount. Cross-volume falls back to copy+delete.
            retryOnLock("move " + temp + " -> " + target,
                    () -> Files.move(temp, target, REPLACE_EXISTING));
        }

        log.info("ship file written: " + target + " (" + Files.size(target) + " bytes)");
    }

    /**
     * Recursive equivalent of {@code find . -name "*.out"}. Sorted for deterministic
     * ordering, which find(1) does not guarantee. Excludes the temp file and the
     * target itself, which the original shell command would have swept up had either
     * lived under the fulfill directory.
     */
    private List<Path> collectOutFiles(Path workDir, Path temp, Path target) throws IOException {
        Path normalizedTemp = temp.toAbsolutePath().normalize();
        Path normalizedTarget = target.toAbsolutePath().normalize();

        try (Stream<Path> walk = Files.walk(workDir)) {
            return walk
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString()
                            .toLowerCase(Locale.ROOT).endsWith(OUT_SUFFIX))
                    .filter(p -> {
                        Path abs = p.toAbsolutePath().normalize();
                        return !abs.equals(normalizedTemp) && !abs.equals(normalizedTarget);
                    })
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /**
     * Streams each source into {@code dest}. Files.copy is chunked, so multi-GB
     * ship files never land in the heap.
     */
    private void concatTo(Path dest, List<Path> sources, java.nio.file.OpenOption... options)
            throws IOException {
        try (OutputStream out = Files.newOutputStream(dest, options)) {
            for (Path src : sources) {
                if (!Files.exists(src)) {
                    throw new NoSuchFileException(src.toString(),
                            null, "required ship file component is missing");
                }
                log.info("appending " + src);
                Files.copy(src, out);
            }
            out.flush();
        }
    }

    /** A file operation that may be transiently blocked by an OS-level lock. */
    @FunctionalInterface
    private interface FileOp {
        void run() throws IOException;
    }

    /**
     * Retries an operation Windows can transiently reject while another process holds
     * a handle (AV scanner, lagging writer). On Linux the first attempt always
     * succeeds, so this costs nothing there.
     *
     * NoSuchFileException is rethrown immediately — a genuinely missing file is not a
     * lock, and retrying it five times just delays the real error.
     */
    private void retryOnLock(String description, FileOp op) throws IOException {
        FileSystemException last = null;
        for (int attempt = 1; attempt <= LOCK_RETRIES; attempt++) {
            try {
                op.run();
                return;
            }
            catch (NoSuchFileException e) {
                throw e;
            }
            catch (FileSystemException e) {   // covers AccessDeniedException
                last = e;
                log.warn(description + " blocked (attempt " + attempt + "/" + LOCK_RETRIES
                        + "): " + e.getMessage());
                try {
                    Thread.sleep(LOCK_RETRY_WAIT_MS * attempt);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while retrying: " + description, ie);
                }
            }
        }
        throw new IOException(description + " failed after " + LOCK_RETRIES + " attempts", last);
    }

    // ------------------------------------------------------------------
    // Existing members below this point are unchanged — merge with your copy.
    // ------------------------------------------------------------------

    // private void generateEncPanCsv(String custCode, String outFileName) { ... }
}
