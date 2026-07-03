public static String getOrCreateSeqIndexForUnknownPan(String csvPath, String realPan, int fieldLength) {

    ConcurrentHashMap<String, String> panMap = PAN_TO_SEQ_INDEX_CACHE.computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>());

    return panMap.computeIfAbsent(realPan, p -> {
        Map<String, String> reverseMap = REVERSE_CACHE.getOrDefault(csvPath, Collections.emptyMap());
        Map<String, String> newToOriginal = NEW_TO_ORIGINAL_INDEX_CACHE.computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>());

        String seqIndex;
        int attempts = 0;
        final int maxAttempts = 1000;

        do {
            seqIndex = nextSequentialIndex(csvPath, fieldLength);
            attempts++;
            if (attempts >= maxAttempts) {
                log.error("unable to find free seqIndex for unknown PAN after [{}] attempts for [{}]", maxAttempts, csvPath);
                throw new IllegalStateException("exhausted seqIndex generation attempts for " + csvPath);
            }
            // must avoid BOTH: already-claimed newIndex entries, AND any real
            // original index in reverseMap that hasn't been processed yet
        } while (newToOriginal.containsKey(seqIndex) || reverseMap.containsValue(seqIndex));

        addNewToOriginalMapping(csvPath, seqIndex, seqIndex);

        Map<String, String> forwardMap = FORWARD_CACHE.computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>());
        forwardMap.put(seqIndex, p);

        log.info("registered unknown PAN not present in input CSV, assigned new seqIndex [{}] for [{}]", seqIndex, csvPath);
        return seqIndex;
    });
}
