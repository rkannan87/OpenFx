public static boolean isSuccess(TransferResult result) {
 
        // ── Java 17: instanceof pattern binding ──────────────────────────────
        if (result instanceof TransferResult.Success s) {
            log.info("[{}] TRANSFER SUCCESS — {} MB in {}s at {:.2f} MB/s encrypted={}",
                    s.transferId(),
                    s.bytesTransferred() / (1024 * 1024),
                    s.elapsed().toSeconds(),
                    s.throughputMBps(),
                    s.encrypted());
            return true;
        }
 
        if (result instanceof TransferResult.Partial p) {
            log.warn("[{}] TRANSFER PARTIAL — {}/{} MB ({:.1f}%) checkpoint={}",
                    p.transferId(),
                    p.bytesTransferred() / (1024 * 1024),
                    p.totalBytes()       / (1024 * 1024),
                    (p.bytesTransferred() * 100.0 / p.totalBytes()),
                    p.checkpointPath());
            return false;
        }
 
        if (result instanceof TransferResult.Failure f) {
            log.error("[{}] TRANSFER FAILED — reason={} cause={}",
                    f.transferId(),
                    f.reason(),
                    f.cause() != null ? f.cause().getMessage() : "none");
            return false;
        }
 
        // Should never reach here — sealed interface covers all cases
        throw new IllegalStateException("Unknown TransferResult type: " + result);
    }
