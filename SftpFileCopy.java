**
     * Returns the byte offset to resume from: the size of an existing partial
     * destination, or 0 for a fresh transfer. A destination larger than the
     * source is stale garbage — restart from zero (with TRUNC).
     */
    private long resolveResumeOffset(SFTPClient writer, String destination, long sourceSize) {
        try {
            FileAttributes attrs = writer.statExistence(destination);
            if (attrs == null) {
                return 0;
            }
            long existing = attrs.getSize();
            if (existing > sourceSize) {
                log.warn("Destination ({} bytes) larger than source ({} bytes) — restarting",
                        existing, sourceSize);
                return 0;
            }
            if (existing < RESUME_THRESHOLD_BYTES) {
                return 0; // cheaper to re-copy small partials than to resume
            }
            return existing;
        } catch (IOException e) {
            log.warn("Could not stat destination for resume check, starting fresh", e);
            return 0;
        }
    }
 
    /**
     * Copies all bytes using a {@link #BUFFER_SIZE} buffer — ~16x fewer loop
     * iterations than transferTo()'s 8–16 KB internal buffer, and each read()
     * drains multiple prefetched packets from the read-ahead stream.
     */
    private static long transfer(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long total = 0;
        int read;
        while ((read = in.read(buffer)) > 0) {
            out.write(buffer, 0, read);
            total += read;
        }
        return total;
    }
