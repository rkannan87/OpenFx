/**
 * Return an existing sequenceIndex for a realPan, or atomically generate,
 * record (new->original mapping), and cache a new one. Same realPan returns
 * the same sequenceIndex for the life of the job (per csvPath). Thread-safe.
 *
 * Records the new->original mapping exactly once per unique PAN — this is
 * what Stage 5 (ShipfileBuilderImpl) consumes to rebuild the plain PAN map.
 *
 * @param csvPath       absolute path to the .encPan.csv file
 * @param realPan       real account number (caller trims)
 * @param originalIndex original input INDEX from reverseMap (non-null)
 * @param fieldLength   account field length for zero-padding
 * @return sequenceIndex (existing or new)
 */
public static String getOrCreateSeqIndex(String csvPath, String realPan,
        String originalIndex, int fieldLength) {

    ConcurrentHashMap<String, String> panMap =
            PAN_TO_SEQ_INDEX_CACHE.computeIfAbsent(csvPath, k -> new ConcurrentHashMap<>());

    return panMap.computeIfAbsent(realPan, p -> {
        String seqIndex = nextSequentialIndex(csvPath, fieldLength);
        addNewToOriginalMapping(csvPath, seqIndex, originalIndex);
        return seqIndex;
    });
}

PAN_TO_SEQ_INDEX_CACHE.remove(csvPath);

PAN_TO_SEQ_INDEX_CACHE.clear();


//PanRestorationHelper.java
/**
 * Substitute the account number in a serialized |AC| row with a job-wide
 * consistent sequenceIndex, WITHOUT mutating any DTO. Operates on the
 * serialized string so the in-memory IndicAccountDto (shared with DB
 * persistence and scoring) keeps its realPan.
 *
 * acctNum is IndicAccountDto parts[4] => 5 tokens after the "AC" token.
 */
public static String substituteAcctNumInRow(String row,
        Map<String, String> reverseMap, String csvPath, int fieldLength) {

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

    int acctNumIdx = acIdx + 5;
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

    int padLen = cols[acctNumIdx].length();
    cols[acctNumIdx] = String.format("%-" + padLen + "s", seqIndex);

    log.debug("txt: realPan [{}] -> seqIndex [{}] col [{}]", acctNum, seqIndex, acctNumIdx);
    return String.join("|", cols);
}

/**
 * Resolve seqIndex back to realPan on the in-memory DTOs, so Stage 4 scoring
 * (which REQUIRES realPan) works after reading a .txt that carries seqIndex.
 * DTOs are later re-substituted to seqIndex for the .out file.
 *
 * seqIndex -> originalIndex (NEW_TO_ORIGINAL_INDEX_CACHE)
 * originalIndex -> realPan  (FORWARD_CACHE)
 */
public static void resolveSeqIndexToRealPan(List<IndicBaseDto> dtos, String csvPath) {
    if (dtos == null || csvPath == null) return;

    Map<String, String> newToOriginal = PanSubstitutionService.getNewToOriginalMap(csvPath);
    Map<String, String> forwardMap     = PanSubstitutionService.getForwardMap(csvPath);
    if (newToOriginal == null || newToOriginal.isEmpty()
            || forwardMap == null || forwardMap.isEmpty()) {
        log.warn("missing seq->orig / forward maps for [{}]; cannot resolve realPan "
                + "for scoring — scores may be incorrect", csvPath);
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
                acct.setAcctNum(realPan);   // in-memory only, for scoring
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
                        csvPath, pan, originalIndex, fieldLength);  // cache HIT = same as .txt
                acct.setAcctNum(seqIndex);
            }
        }
    }
}

// inside restoreConsumerRow, replace the index-generation block with:
String newIndex = PanSubstitutionService.getOrCreateSeqIndex(
        csvPath, realPan, originalIndex, accountLength);
// (removes the separate nextSequentialIndex + addNewToOriginalMapping calls)

//SaveTriggeredConsumerDataImpl.java
public void saveConsumerData(ChunkMessage cm,
        Map<Integer, List<FiredTriggerResult>> firedTriggerMap,
        Map<Integer, List<IndicBaseDto>> fidMap) throws TucException {

    Object fileHandle = null;
    String lineseperator = System.getProperty("line.separator");

    String encPanCsvPath = resolveEncPanCsvPath(cm);
    Map<String, String> reverseMap = null;
    int fieldLengthRp3 = 16;
    if (encPanCsvPath != null) {
        PanSubstitutionService panService = new PanSubstitutionService(
                new EncPanCsvReader(), PanDecryptServiceHolder.getInstance());
        reverseMap = panService.loadReverseMap(encPanCsvPath);
        int[] acctFieldRp3 = PanSubstitutionService.getAccountFieldInfo(encPanCsvPath);
        if (acctFieldRp3 != null) fieldLengthRp3 = acctFieldRp3[1];
    }
    boolean doSubstitute = (reverseMap != null && !reverseMap.isEmpty());
    if (!doSubstitute && encPanCsvPath != null) {
        log.warn("PAN reverseMap empty for [" + encPanCsvPath
                + "]; .txt may contain realPan, chunk [" + cm.getChunkId() + "]");
    }

    try {
        if (firedTriggerMap == null) {
            log.info("no triggers fired for chunk [" + cm.getChunkId() + "]");
            return;
        }

        String fileName = dirHelper.getAbsoluteFullfillName(cm);
        fileHandle = fileio.openOutput(fileName);

        for (Iterator<Integer> it = firedTriggerMap.keySet().iterator(); it.hasNext(); ) {
            Integer cfid = it.next();
            List<IndicBaseDto> dtos = fidMap.get(cfid);
            List<FiredTriggerResult> results = firedTriggerMap.get(cfid);

            StringBuilder stuff = new StringBuilder();
            String scfid = cfid.toString();

            stuff.append(scfid).append("||A0").append(lineseperator);

            TriggeredResultWrapper trw = new TriggeredResultWrapper();
            for (FiredTriggerResult result : results) {
                stuff.append(trw.toString(scfid, result));
            }

            for (IndicBaseDto dto : dtos) {
                String line = scfid + "|" + dto.toString();   // DTO NOT mutated
                if (doSubstitute) {
                    line = PanRestorationHelper.substituteAcctNumInRow(
                            line, reverseMap, encPanCsvPath, fieldLengthRp3);
                }
                stuff.append(line).append(lineseperator);
            }

            stuff.append(scfid).append("||ZZ").append(lineseperator);
            fileio.writePerson(fileHandle, stuff.toString());
        }
    } catch (Exception e) {
        log.error(e.getMessage(), e);
    } finally {
        if (fileHandle != null) fileio.close(fileHandle);
    }
}

private String resolveEncPanCsvPath(ChunkMessage cm) {
    String csvPath = PanSubstitutionService.getCompanyCsvPath(cm.getCompany());
    if (csvPath == null) return null;
    File csvFile = new File(csvPath);
    return csvFile.exists() ? csvPath : null;
}

//WorkerFullfill.java
private void processDtoSegments(EntityManager em, Counts counts,
        ChunkMessage cm, Object fileHandleInput, String fileName) {
    try {
        String encPanCsvPath = resolveEncPanCsvPath(cm);

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
