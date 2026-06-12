 /**
     * Streams source -> destination, resuming from a partial destination if one exists.
     */
    private boolean streamCopyWithResume(PooledSshClient readerClient,
                                         PooledSshClient writerClient,
                                         String sourceFile, String destination,
                                         long setupMillis) {
        try (SFTPClient reader = readerClient.newSFTPClient();
             SFTPClient writer = writerClient.newSFTPClient()) {
 
            long sourceSize = reader.stat(sourceFile).getSize();
            long resumeOffset = resolveResumeOffset(writer, destination, sourceSize);
 
            if (resumeOffset == sourceSize) {
                log.info("Destination already complete ({} bytes), skipping copy", sourceSize);
                return true;
            }
            if (resumeOffset > 0) {
                log.info("Resuming transfer at offset {} of {} ({}%)",
                        resumeOffset, sourceSize, (resumeOffset * 100) / sourceSize);
            }
 
            // No TRUNC when resuming — we append from the offset.
            EnumSet<OpenMode> writeModes = resumeOffset > 0
                    ? EnumSet.of(OpenMode.WRITE, OpenMode.CREAT)
                    : EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.TRUNC);
 
            try (RemoteFile src = reader.open(sourceFile, EnumSet.of(OpenMode.READ));
                 RemoteFile dst = writer.open(destination, writeModes);
                 // SFTP reads are offset-addressed: starting the stream AT the resume
                 // offset means the resumed bytes are never re-downloaded. The old
                 // skipFully() approach pulled them all through the network to discard.
                 InputStream in = src.new ReadAheadRemoteFileInputStream(
                         READ_AHEAD_REQUESTS, resumeOffset);
                 OutputStream out = dst.new RemoteFileOutputStream(resumeOffset, READ_AHEAD_REQUESTS)) {
 
                long transferStart = System.nanoTime();
                long transferred = transfer(in, out);
                long transferMillis = (System.nanoTime() - transferStart) / 1_000_000;
 
                logTransferMetrics("stream", sourceFile, destination,
                        transferred, setupMillis, transferMillis, resumeOffset);
                return true;
            }
        } catch (IOException e) {
            log.error("Error trying to SFTP copy source file: {} to destination file: {}",
                    sourceFile, destination, e);
            return false;
        }
    }
