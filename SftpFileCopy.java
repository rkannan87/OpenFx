private List<String> processCompanyFile(String landingDir, String fname, String company)
        throws IOException {

    String filename = landingDir + fname;
    File foundFile = new File(filename);
    SimpleDateFormat sdf = new SimpleDateFormat();
    sdf.applyPattern("yyyyMMdd.HHmmss");

    // ── strip the .complete ingestion marker before timestamping ──
    String baseName = foundFile.getName();          // cust_input.txt.complete
    final String COMPLETE = ".complete";
    if (baseName.endsWith(COMPLETE)) {
        baseName = baseName.substring(0, baseName.length() - COMPLETE.length());  // cust_input.txt
    }

    String newname = baseName + "." + sdf.format(new Date());   // cust_input.txt.20260617.164036
    List<String> fileNames = new ArrayList<>();

    if (getSplitRowCount() == 0) {
        String newfullname = dirHelper.getAbsoluteSearchDir(company) + newname;
        log.info("rename file [" + filename + "] to [" + newfullname + "]");
        File newfile = new File(newfullname);
        boolean isRenames = foundFile.renameTo(newfile);
        boolean exists = newfile.exists();
        if (!isRenames || !exists) {
            log.error("unable to rename to [" + newfullname + "]");
        } else {
            fileNames.add(newname);
        }
    } else {
        processFile(filename, fileNames, newname, company);
        String newfullname = dirHelper.getAbsoluteSearchCompletedDir(company) + newname;
        String message = "rename file [" + filename + "] to [" + newfullname + "]";
        log.info(message);
        boolean rc = foundFile.renameTo(new File(newfullname));
        if (rc) log.info(message + " successfull");
        else log.error(message + " failed");
    }

    // Move companion .encPan.csv to search dir if it exists (with matching timestamp)
    moveEncPanCsvToSearchDir(landingDir, baseName, company, newname);   // ← pass STRIPPED base

    return fileNames;
}


private void moveEncPanCsvToSearchDir(String landingDir, String inputFileName,
        String company, String newname) {

    // landing companion carries the .complete marker AFTER .EncPan.csv:
    //   cust_input.txt.EncPan.csv.complete
    String csvName = inputFileName + ".EncPan.csv.complete";
    File csvSource = new File(landingDir + csvName);

    if (!csvSource.exists()) {
        // fallback for deliveries without the marker
        File alt = new File(landingDir + inputFileName + ".EncPan.csv");
        if (alt.exists()) {
            csvSource = alt;
        } else {
            log.debug("no encPan CSV found [" + csvSource.getAbsolutePath()
                    + "], skipping PAN substitution");
            return;
        }
    }

    // newname = cust_input.txt.20260617.164036 ; inputFileName = cust_input.txt
    String timestamp = newname.substring(inputFileName.length() + 1);   // 20260617.164036

    // destination drops .complete, keeps old convention downstream expects:
    //   cust_input.txt.20260617.164036.EncPan.csv
    String timestampedCsvName = inputFileName + "." + timestamp + ".EncPan.csv";

    File csvDest = new File(dirHelper.getAbsoluteSearchDir(company) + timestampedCsvName);
    boolean rc = csvSource.renameTo(csvDest);

    if (rc) {
        log.info("moved encPan CSV [" + csvSource.getName() + "] to search dir ["
                + csvDest.getAbsolutePath() + "]");
    } else {
        log.error("unable to move encPan CSV [" + csvSource.getAbsolutePath()
                + "] to [" + csvDest.getAbsolutePath() + "]");
    }
}
