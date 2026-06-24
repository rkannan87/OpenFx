/**
 * Substitute the account number in a single serialized |AC| row with a
 * job-wide consistent sequenceIndex, WITHOUT mutating any DTO.
 *
 * Anchors on the "AC" token. acctNum is 3 tokens after "AC"
 * (AC | subId | ref | acctNum), which is correct for BOTH the
 * "cfid||TD|fid|AC|..." notification rows and the "cfid|fid|AC|..."
 * consumer rows because we anchor on "AC", not on a fixed column.
 *
 * @param row         serialized consumer row (single line)
 * @param reverseMap  realPan -> originalIndex
 * @param csvPath     absolute path to the .encPan.csv file
 * @param fieldLength account field length for padding
 * @return row with acctNum replaced by sequenceIndex, or unchanged row
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
        if ("AC".equals(cols[i].trim())) {
            acIdx = i;
            break;
        }
    }
    if (acIdx == -1) {
        return row;
    }

    // AC | subId | ref | acctNum  =>  acctNum at acIdx + 3
    int acctNumIdx = acIdx + 3;
    if (acctNumIdx >= cols.length) {
        return row;
    }

    String acctNum = cols[acctNumIdx].trim();
    if (acctNum.isEmpty()) {
        return row;
    }

    String originalIndex = reverseMap.get(acctNum);
    if (originalIndex == null) {
        log.warn("acctNum [{}] not in reverseMap for [{}]; row left "
                + "unsubstituted (realPan remains in .txt)", acctNum, csvPath);
        return row;
    }

    String seqIndex = PanSubstitutionService.getOrCreateSeqIndex(
            csvPath, acctNum, originalIndex, fieldLength);

    int padLen = cols[acctNumIdx].length();
    cols[acctNumIdx] = String.format("%-" + padLen + "s", seqIndex);

    log.debug("txt: realPan [{}] -> seqIndex [{}] col [{}]",
            acctNum, seqIndex, acctNumIdx);

    return String.join("|", cols);
}

/**
 * Substitute the account number in every |AC| line within a multi-line
 * block (e.g. the output of TriggeredResultWrapper.toString, which emits
 * the ||TD| notification rows). Non-AC lines are left untouched.
 * Preserves original line separators (handles \n and \r\n).
 *
 * @param block       multi-line serialized block
 * @param reverseMap  realPan -> originalIndex
 * @param csvPath     absolute path to the .encPan.csv file
 * @param fieldLength account field length for padding
 * @return block with all AC-row acctNums substituted
 */
public static String substituteAcLinesInBlock(String block,
        Map<String, String> reverseMap, String csvPath, int fieldLength) {

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
        if (hasCr) {
            contentEnd--;
        }

        String line = block.substring(start, contentEnd);

        if (line.contains("|AC|")) {
            line = substituteAcctNumInRow(line, reverseMap, csvPath, fieldLength);
        }

        out.append(line);
        if (hasCr) {
            out.append('\r');
        }
        if (nl >= 0) {
            out.append('\n');
        }

        start = (nl < 0) ? len : nl + 1;
    }

    return out.toString();
}

public void saveConsumerData(ChunkMessage cm,
        Map<Integer, List<FiredTriggerResult>> firedTriggerMap,
        Map<Integer, List<IndicBaseDto>> fidMap) throws TucException {

    Object fileHandle = null;
    String lineseperator = System.getProperty("line.separator");

    // ── PAN substitution setup (cached per job; cheap after first chunk) ──
    String encPanCsvPath = resolveEncPanCsvPath(cm);
    Map<String, String> reverseMap = null;
    int fieldLengthRp3 = 16;
    if (encPanCsvPath != null) {
        PanSubstitutionService panService = new PanSubstitutionService(
                new EncPanCsvReader(), PanDecryptServiceHolder.getInstance());
        reverseMap = panService.loadReverseMap(encPanCsvPath);

        int[] acctFieldRp3 = PanSubstitutionService.getAccountFieldInfo(encPanCsvPath);
        if (acctFieldRp3 != null) {
            fieldLengthRp3 = acctFieldRp3[1];
        }
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

            // ── Block A: triggered notification rows (||TD|) ──
            TriggeredResultWrapper trw = new TriggeredResultWrapper();
            for (FiredTriggerResult result : results) {
                String trigBlock = trw.toString(scfid, result);

                if (doSubstitute && trigBlock.contains("|AC|")) {
                    trigBlock = PanRestorationHelper.substituteAcLinesInBlock(
                            trigBlock, reverseMap, encPanCsvPath, fieldLengthRp3);
                }

                stuff.append(trigBlock);
            }

            // ── Block B: consumer DTO rows (single-pipe) ──
            for (IndicBaseDto dto : dtos) {
                String line = scfid + "|" + dto.toString();   // DTO NOT mutated

                if (doSubstitute) {
                    line = PanRestorationHelper.substituteAcctNumInRow(
                            line, reverseMap, encPanCsvPath, fieldLengthRp3);
                }

                stuff.append(line).append(lineseperator);
            }

            // terminator
            stuff.append(scfid).append("||ZZ").append(lineseperator);

            fileio.writePerson(fileHandle, stuff.toString());
        }

    } catch (Exception e) {
        log.error(e.getMessage(), e);
    } finally {
        if (fileHandle != null) {
            fileio.close(fileHandle);
        }
    }
}

/**
 * Derive the .encPan.csv path from the chunk message.
 * @return absolute path to the .encPan.csv file, or null if it doesn't exist
 */
private String resolveEncPanCsvPath(ChunkMessage cm) {
    String csvPath = PanSubstitutionService.getCompanyCsvPath(cm.getCompany());
    if (csvPath == null) {
        return null;
    }
    File csvFile = new File(csvPath);
    return csvFile.exists() ? csvPath : null;
}
