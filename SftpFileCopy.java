PanSubstitutionService
ca.tuc.triggers.processor.pan

/** realPan -> sequenceIndex, per csvPath (job-wide). Same PAN reuses same INDEX. */
private static final ConcurrentHashMap<String, ConcurrentHashMap<String, String>>
        PAN_TO_SEQ_INDEX_CACHE = new ConcurrentHashMap<>();

/**
 * Return an existing sequenceIndex for a realPan, or atomically generate,
 * record (new->original mapping), and cache a new one. Same realPan returns
 * the same sequenceIndex for the life of the job (per csvPath). Thread-safe.
 *
 * @param csvPath       absolute path to the .encPan.csv file
 * @param realPan       real account number (caller trims)
 * @param originalIndex original input INDEX from reverseMap (non-null)
 * @param fieldLength   account field length for zero-padding
 * @return sequenceIndex, or null if originalIndex is null
 */
public static String getOrCreateSeqIndex(String csvPath, String realPan,
        String originalIndex, int fieldLength) {

    if (originalIndex == null) {
        log.warn("getOrCreateSeqIndex called with null originalIndex for [{}]; skipping", csvPath);
        return null;
    }

    ConcurrentHashMap<String, String> panMap =
            PAN_TO_SEQ_INDEX_CACHE.computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>());

    return panMap.computeIfAbsent(realPan, p -> {
        String seqIndex = nextSequentialIndex(csvPath, fieldLength);
        addNewToOriginalMapping(csvPath, seqIndex, originalIndex);
        return seqIndex;
    });
}

/**
 * Seed caches on recovery from a companion CSV's seqIndex -> realPan pair.
 * The original-index hop is collapsed: the seqIndex is its own resolution key,
 * so generateEncPanCsv (newToOriginal -> forward) resolves correctly.
 * Idempotent via putIfAbsent.
 */
public static void restoreRecoveredMapping(String csvPath,
        String seqIndex, String realPan) {

    if (csvPath == null || seqIndex == null || realPan == null) {
        return;
    }

    NEW_TO_ORIGINAL_INDEX_CACHE
        .computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>())
        .putIfAbsent(seqIndex, seqIndex);

    FORWARD_CACHE
        .computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>())
        .putIfAbsent(seqIndex, realPan);

    PAN_TO_SEQ_INDEX_CACHE
        .computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>())
        .putIfAbsent(realPan, seqIndex);
}

/**
 * Rebuild PAN caches for a crashed chunk from its companion .encPan.csv
 * (written next to the fullfill .txt at Stage 3). Reads seqIndex -> ENC(realPan),
 * decrypts, seeds the caches, and advances the sequential counter past the
 * highest recovered seqIndex so post-recovery generation cannot reissue one.
 *
 * @param txtPath absolute path of the fullfill .txt (companion = txtPath + ".encPan.csv")
 * @param csvPath the job's input encPan csv path (cache key)
 * @return number of mappings recovered
 */
public static int recoverChunkFromCompanionCsv(String txtPath, String csvPath) {
    String companion = txtPath + ".encPan.csv";
    File f = new File(companion);
    if (!f.exists()) {
        log.warn("no companion encPan CSV [{}]; cannot recover seqIndex mappings", companion);
        return 0;
    }
    try {
        PanLookup lookup = new EncPanCsvReader().parse(companion);
        Map<String, String> seqToEnc = lookup.getIndexToEncValue();

        PanSubstitutionService svc = new PanSubstitutionService(
                new EncPanCsvReader(), PanDecryptServiceHolder.getInstance());
        Map<String, String> seqToRealPan =
                svc.decryptAllAESKey(lookup.getKeyLocation(), seqToEnc);

        long maxSeq = 0L;
        for (Map.Entry<String, String> e : seqToRealPan.entrySet()) {
            String seqIndex = e.getKey();
            restoreRecoveredMapping(csvPath, seqIndex, e.getValue());
            try {
                long n = Long.parseLong(seqIndex.trim());
                if (n > maxSeq) maxSeq = n;
            } catch (NumberFormatException nfe) {
                log.debug("non-numeric recovered seqIndex [{}]; skipped for counter advance", seqIndex);
            }
        }

        final long advanceTo = maxSeq;
        SEQUENTIAL_COUNTER_CACHE
            .computeIfAbsent(csvPath, k -> new AtomicLong(0L))
            .updateAndGet(cur -> Math.max(cur, advanceTo));

        log.info("recovered [{}] mappings from [{}]; counter advanced to [{}]",
                seqToRealPan.size(), companion, advanceTo);
        return seqToRealPan.size();

    } catch (Exception e) {
        log.error("failed to recover from companion encPan CSV [" + companion + "]", e);
        return 0;
    }
}

public void evict(String csvPath) {
    Map<String, String> fwd = FORWARD_CACHE.remove(csvPath);
    Map<String, String> rev = REVERSE_CACHE.remove(csvPath);
    ACCOUNT_FIELD_CACHE.remove(csvPath);
    OUTPUT_ACCOUNT_FIELD_CACHE.remove(csvPath);
    KEY_LOCATION_CACHE.remove(csvPath);
    SEQUENTIAL_COUNTER_CACHE.remove(csvPath);
    NEW_TO_ORIGINAL_INDEX_CACHE.remove(csvPath);
    PAN_TO_SEQ_INDEX_CACHE.remove(csvPath);
    COMPANY_CSV_PATH_CACHE.entrySet().removeIf(e -> csvPath.equals(e.getValue()));

    log.info("evicted PAN caches for [{}] forward={} reverse={}",
            csvPath, fwd != null ? fwd.size() : 0, rev != null ? rev.size() : 0);
}
//clearAllCaches()
PAN_TO_SEQ_INDEX_CACHE.clear();

PanRestorationHelper
/**
 * Substitute the account number in a serialized |AC| row with a job-wide
 * consistent sequenceIndex, WITHOUT mutating any DTO. acctNum is 3 tokens
 * after the "AC" token (AC|subId|ref|acctNum). When seqToRealPanOut is
 * non-null, records seqIndex -> realPan for the per-chunk recovery CSV.
 */
public static String substituteAcctNumInRow(String row,
        Map<String, String> reverseMap, String csvPath, int fieldLength,
        Map<String, String> seqToRealPanOut) {

    if (row == null || reverseMap == null || reverseMap.isEmpty()
            || !row.contains("|AC|")) {
        return row;
    }
    String[] cols = row.split("\\|", -1);

    int acIdx = -1;
    for (int i = 0; i < cols.length; i++) {
        if ("AC".equals(cols[i].trim())) { acIdx = i; break; }
    }
    if (acIdx == -1) return row;

    int acctNumIdx = acIdx + 3;
    if (acctNumIdx >= cols.length) return row;

    String acctNum = cols[acctNumIdx].trim();
    if (acctNum.isEmpty()) return row;

    String originalIndex = reverseMap.get(acctNum);
    if (originalIndex == null) {
        log.warn("acctNum [{}] not in reverseMap for [{}]; row left unsubstituted "
                + "(realPan remains in .txt)", acctNum, csvPath);
        return row;
    }

    String seqIndex = PanSubstitutionService.getOrCreateSeqIndex(
            csvPath, acctNum, originalIndex, fieldLength);
    if (seqIndex == null) return row;

    if (seqToRealPanOut != null) {
        seqToRealPanOut.put(seqIndex, acctNum);   // acctNum == realPan
    }

    int padLen = cols[acctNumIdx].length();
    cols[acctNumIdx] = String.format("%-" + padLen + "s", seqIndex);

    log.debug("txt: realPan [{}] -> seqIndex [{}] col [{}]", acctNum, seqIndex, acctNumIdx);
    return String.join("|", cols);
}

/** Backward-compatible overload (no mapping collection). */
public static String substituteAcctNumInRow(String row,
        Map<String, String> reverseMap, String csvPath, int fieldLength) {
    return substituteAcctNumInRow(row, reverseMap, csvPath, fieldLength, null);
}

/**
 * Substitute every |AC| line within a multi-line block (TriggeredResultWrapper
 * output / ||TD| notification rows). Preserves line separators. Collects
 * seqIndex -> realPan when seqToRealPanOut is non-null.
 */
public static String substituteAcLinesInBlock(String block,
        Map<String, String> reverseMap, String csvPath, int fieldLength,
        Map<String, String> seqToRealPanOut) {

    if (block == null || block.isEmpty() || !block.contains("|AC|")) {
        return block;
    }

    StringBuilder out = new StringBuilder(block.length() + 16);
    int start = 0;
    int len = block.length();

    while (start < len) {
        int nl = block.indexOf('\n', start);
        int lineEnd = (nl < 0) ? len : nl;

        int contentEnd = lineEnd;
        boolean hasCr = (contentEnd > start && block.charAt(contentEnd - 1) == '\r');
        if (hasCr) contentEnd--;

        String line = block.substring(start, contentEnd);
        if (line.contains("|AC|")) {
            line = substituteAcctNumInRow(line, reverseMap, csvPath, fieldLength, seqToRealPanOut);
        }

        out.append(line);
        if (hasCr) out.append('\r');
        if (nl >= 0) out.append('\n');

        start = (nl < 0) ? len : nl + 1;
    }
    return out.toString();
}

/** Backward-compatible overload (no mapping collection). */
public static String substituteAcLinesInBlock(String block,
        Map<String, String> reverseMap, String csvPath, int fieldLength) {
    return substituteAcLinesInBlock(block, reverseMap, csvPath, fieldLength, null);
}

/**
 * Resolve seqIndex back to realPan on in-memory DTOs so Stage 4 scoring
 * (which requires realPan) works after reading a .txt that carries seqIndex.
 */
public static void resolveSeqIndexToRealPan(List<IndicBaseDto> dtos, String csvPath) {
    if (dtos == null || csvPath == null) return;

    Map<String, String> newToOriginal = PanSubstitutionService.getNewToOriginalMap(csvPath);
    Map<String, String> forwardMap     = PanSubstitutionService.getForwardMap(csvPath);
    if (newToOriginal == null || newToOriginal.isEmpty()
            || forwardMap == null || forwardMap.isEmpty()) {
        log.warn("missing seq->orig / forward maps for [{}]; cannot resolve realPan", csvPath);
        return;
    }

    for (IndicBaseDto dto : dtos) {
        if (dto instanceof IndicAccountDto) {
            IndicAccountDto acct = (IndicAccountDto) dto;
            String seqIndex = acct.getAcctNum();
            if (seqIndex == null || seqIndex.trim().isEmpty()) continue;

            String originalIndex = newToOriginal.get(seqIndex.trim());
            if (originalIndex == null) continue;

            String realPan = forwardMap.get(originalIndex);
            if (realPan != null) {
                acct.setAcctNum(realPan);
                log.debug("resolved seqIndex [{}] -> realPan for scoring", seqIndex);
            }
        }
    }
}

public static void restoreIndicDtoList(List<IndicBaseDto> dtos,
        Map<String, String> reverseMap, String csvPath, int fieldLength) {

    if (dtos == null || reverseMap == null || reverseMap.isEmpty()) return;

    for (IndicBaseDto dto : dtos) {
        if (dto instanceof IndicAccountDto) {
            IndicAccountDto acct = (IndicAccountDto) dto;
            String acctNum = acct.getAcctNum();
            if (acctNum == null || acctNum.trim().isEmpty()) continue;

            String pan = acctNum.trim();
            String originalIndex = reverseMap.get(pan);
            if (originalIndex != null) {
                String seqIndex = PanSubstitutionService.getOrCreateSeqIndex(
                        csvPath, pan, originalIndex, fieldLength);
                if (seqIndex != null) acct.setAcctNum(seqIndex);
            }
        }
    }
}

ca.tuc.triggers.processor.triggers
SaveTriggeredConsumerDataImpl
public void saveConsumerData(ChunkMessage cm,
        Map<Integer, List<FiredTriggerResult>> firedTriggerMap,
        Map<Integer, List<IndicBaseDto>> fidMap) throws TucException {

    Object fileHandle = null;
    String lineseperator = System.getProperty("line.separator");

    String encPanCsvPath = resolveEncPanCsvPath(cm);
    Map<String, String> reverseMap = null;
    String keyLocation = null;
    int[] outputAcctField = null;
    int fieldLengthRp3 = 16;

    if (encPanCsvPath != null) {
        PanSubstitutionService panService = new PanSubstitutionService(
                new EncPanCsvReader(), PanDecryptServiceHolder.getInstance());
        reverseMap      = panService.loadReverseMap(encPanCsvPath);
        keyLocation     = PanSubstitutionService.getKeyLocation(encPanCsvPath);
        outputAcctField = PanSubstitutionService.getOutputAccountFieldInfo(encPanCsvPath);
        int[] acctFieldRp3 = PanSubstitutionService.getAccountFieldInfo(encPanCsvPath);
        if (acctFieldRp3 != null) fieldLengthRp3 = acctFieldRp3[1];
    }
    boolean doSubstitute = (reverseMap != null && !reverseMap.isEmpty());
    if (!doSubstitute && encPanCsvPath != null) {
        log.warn("PAN reverseMap empty for [" + encPanCsvPath
                + "]; .txt may contain realPan, chunk [" + cm.getChunkId() + "]");
    }

    Map<String, String> chunkSeqToRealPan = new LinkedHashMap<>();

    String txtPath = dirHelper.getAbsoluteFullfillName(cm);
    String txtTmp  = txtPath + ".tmp";

    try {
        if (firedTriggerMap == null) {
            log.info("no triggers fired for chunk [" + cm.getChunkId() + "]");
            return;
        }

        fileHandle = fileio.openOutput(txtTmp);

        for (Iterator<Integer> it = firedTriggerMap.keySet().iterator(); it.hasNext(); ) {
            Integer cfid = it.next();
            List<IndicBaseDto> dtos = fidMap.get(cfid);
            List<FiredTriggerResult> results = firedTriggerMap.get(cfid);

            StringBuilder stuff = new StringBuilder();
            String scfid = cfid.toString();
            stuff.append(scfid).append("||A0").append(lineseperator);

            // Block A — TD notification rows (may contain |AC|)
            TriggeredResultWrapper trw = new TriggeredResultWrapper();
            for (FiredTriggerResult result : results) {
                String block = trw.toString(scfid, result);
                if (doSubstitute && block.contains("|AC|")) {
                    block = PanRestorationHelper.substituteAcLinesInBlock(
                            block, reverseMap, encPanCsvPath, fieldLengthRp3, chunkSeqToRealPan);
                }
                stuff.append(block);
            }

            // Block B — consumer DTO rows
            for (IndicBaseDto dto : dtos) {
                String line = scfid + "|" + dto.toString();   // DTO NOT mutated
                if (doSubstitute) {
                    line = PanRestorationHelper.substituteAcctNumInRow(
                            line, reverseMap, encPanCsvPath, fieldLengthRp3, chunkSeqToRealPan);
                }
                stuff.append(line).append(lineseperator);
            }

            stuff.append(scfid).append("||ZZ").append(lineseperator);
            fileio.writePerson(fileHandle, stuff.toString());
        }

    } catch (Exception e) {
        log.error(e.getMessage(), e);
        throw new TucException("failed writing fullfill file for chunk ["
                + cm.getChunkId() + "]", e);
    } finally {
        if (fileHandle != null) {
            try { fileio.close(fileHandle); }
            catch (Exception ce) { log.error("error closing fullfill temp file", ce); }
        }
    }

    // crash-safe promotion: companion CSV first, then .txt last
    try {
        if (doSubstitute && !chunkSeqToRealPan.isEmpty()) {
            writeIntermediateEncPanCsv(txtPath, keyLocation,
                    chunkSeqToRealPan, outputAcctField, cm);
        }
        promote(txtTmp, txtPath);
    } catch (Exception e) {
        log.error("failed finalizing fullfill artifacts for chunk ["
                + cm.getChunkId() + "]", e);
        throw new TucException("failed finalizing fullfill artifacts for chunk ["
                + cm.getChunkId() + "]", e);
    }
}

/**
 * Write a per-chunk companion .encPan.csv next to the fullfill .txt so the
 * seqIndex -> realPan mapping survives a crash/restart between Stage 3 and
 * Stage 5. Format matches Stage 5 output; readable by EncPanCsvReader on recovery.
 */
private void writeIntermediateEncPanCsv(String txtPath, String keyLocation,
        Map<String, String> seqToRealPan, int[] outputAcctField, ChunkMessage cm) {
    String csvPath = txtPath + ".encPan.csv";
    String csvTmp  = csvPath + ".tmp";
    try {
        PanDecryptService decryptService = PanDecryptServiceHolder.getInstance();
        Map<String, String> encryptedMap = (decryptService != null)
                ? decryptService.encryptAll(keyLocation, seqToRealPan)
                : seqToRealPan;

        EncPanCsvWriter.write(csvTmp, keyLocation, encryptedMap, outputAcctField);
        promote(csvTmp, csvPath);

        log.info("wrote intermediate encPan CSV [" + csvPath + "] with ["
                + encryptedMap.size() + "] entries for chunk [" + cm.getChunkId() + "]");
    } catch (Exception e) {
        log.error("failed to write intermediate encPan CSV [" + csvPath
                + "] for chunk [" + cm.getChunkId() + "]", e);
        throw new RuntimeException(e);   // prevents .txt promotion on failure
    }
}

/** Atomically promote a temp file to its final name. */
private void promote(String tmpPath, String finalPath) throws TucException {
    File tmp = new File(tmpPath);
    File dst = new File(finalPath);
    if (dst.exists() && !dst.delete()) {
        log.warn("could not delete existing [" + finalPath + "] before promote");
    }
    if (!tmp.renameTo(dst)) {
        throw new TucException("failed to promote [" + tmpPath + "] -> [" + finalPath + "]");
    }
}

private String resolveEncPanCsvPath(ChunkMessage cm) {
    String csvPath = PanSubstitutionService.getCompanyCsvPath(cm.getCompany());
    if (csvPath == null) return null;
    File csvFile = new File(csvPath);
    return csvFile.exists() ? csvPath : null;
}

WorkerFullfill
ca.tuc.triggers.mainline.workers
private void processDtoSegments(EntityManager em, Counts counts,
        ChunkMessage cm, Object fileHandleInput, String fileName) {
    try {
        String encPanCsvPath = resolveEncPanCsvPath(cm);

        // crash recovery: if caches were lost on restart, rebuild from companion CSV
        if (encPanCsvPath != null) {
            Map<String, String> n2o = PanSubstitutionService.getNewToOriginalMap(encPanCsvPath);
            if (n2o == null || n2o.isEmpty()) {
                int n = PanSubstitutionService.recoverChunkFromCompanionCsv(fileName, encPanCsvPath);
                log.info("recovered [{}] mappings for chunk [{}] after restart", n, cm.getChunkId());
            }
        }

        List<Object> dtoSegments;
        while (((dtoSegments = fileio.readPerson(fileHandleInput)) != null)
                && (!dtoSegments.isEmpty())) {

            em.getTransaction().begin();
            counts.persons++;

            // .txt carries seqIndex; restore realPan for scoring (Stage 4 needs it)
            if (encPanCsvPath != null) {
                List<IndicBaseDto> indicDtos = ListDtoHelper.getByType(
                        dtoSegments, "ca.tuc.commons.dto.schema.indic.IndicBaseDto");
                PanRestorationHelper.resolveSeqIndexToRealPan(indicDtos, encPanCsvPath);
            }

            processFullfiller(cm, dtoSegments, counts, em);
            postBilling(cm, dtoSegments);
            postInquiry(cm, dtoSegments);
            counts.inquiryCount++;
            em.getTransaction().commit();
        }
    } catch (Exception e) {
        log.error(StringEscapeUtils.escapeJava(
            "Problem while processing file [" + fileName + "] "), e);
        em.getTransaction().rollback();
    }
}

private String resolveEncPanCsvPath(ChunkMessage cm) {
    String csvPath = PanSubstitutionService.getCompanyCsvPath(cm.getCompany());
    if (csvPath == null) return null;
    File csvFile = new File(csvPath);
    return csvFile.exists() ? csvPath : null;
}
ShipfileBuilderImpl
ca.tuc.triggers.processor.shipfile
private long currentJobNum;
this.currentJobNum = jobnum;

EncPanCsvWriter.write(encPanOutPath, keyLocation, encryptedMap, outputAccountFieldInfo);

// Stage 5 output committed — delete per-chunk recovery companions for this job
deleteIntermediateEncPanCsvs(custCode);

// existing eviction + input csv delete
PanSubstitutionService panService = new PanSubstitutionService(
        new EncPanCsvReader(), PanDecryptServiceHolder.getInstance());
panService.evict(csvPath);
if (!csvFile.delete()) {
    log.warn("unable to delete encPan CSV [" + csvPath + "] after Stage 5 output written");
} else {
    log.info("deleted encPan CSV [" + csvPath + "] after Stage 5 output written");
}

/**
 * Delete all per-chunk companion .txt.encPan.csv files for this job once the
 * final Stage 5 output CSV is written. Job-scoped so a Stage 5 failure never
 * removes a recovery artifact prematurely.
 */
private void deleteIntermediateEncPanCsvs(String custCode) {
    try {
        String fullfillDir = dirHelper.getAbsoluteFullfillDir(custCode, currentJobNum);
        File dir = new File(fullfillDir);
        File[] companions = dir.listFiles((d, name) -> name.endsWith(".txt.encPan.csv"));
        if (companions == null) return;

        for (File c : companions) {
            if (c.delete()) {
                log.info("deleted intermediate encPan CSV [" + c.getAbsolutePath() + "]");
            } else {
                log.warn("unable to delete intermediate encPan CSV [" + c.getAbsolutePath() + "]");
            }
        }
    } catch (Exception e) {
        log.error("error deleting intermediate encPan CSVs for cust [" + custCode + "]", e);
    }
}
