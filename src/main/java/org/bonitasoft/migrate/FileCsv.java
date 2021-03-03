package org.bonitasoft.migrate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class FileCsv {

    static Logger logger = Logger.getLogger(ImportV7.class.getName());

    String importPath;
    String importedPath;

    public FileCsv(String importPath, String importedPath) {
        this.importPath = importPath;
        this.importedPath = importedPath;

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * manage CSV
     */
    /* ******************************************************************************** */
    public static String TYPELINE = "TypeLine";
    public static String TYPECASEACTIVE = "P_A";
    public static String TYPECASEARCHIVED = "P_F";
    public static String CASEID = "CaseId";
    public static String STARTEDBY = "StartedBy";
    public static String STARTEDDATE = "StartedDate";
    public static String TASKS = "Tasks";
    public static String RENDEZVOUS = "RendezVous";
    public static String TASKSEPARATOR = "#";

    public static String VARIABLES = "Variables";
    public static String VARIABLESACTIVITY = "VariablesActivity";
    public static String SUBPROCESSID = "SubProcessInstance";

    public static List<String> getListFiles(String filePath) {
        List<String> listFiles = new ArrayList<String>();
        File folder = new File(filePath);
        for (final File fileEntry : folder.listFiles()) {
            if (!fileEntry.isDirectory()) {
                if (fileEntry.getName().endsWith(".csv"))
                    listFiles.add(fileEntry.getName());
            }
        }
        return listFiles;
    }

    /**
     * return the number of line in this CSV file
     * 
     * @param fileName
     * @return
     */
    public static long getNumberCsvLine(String fileName) {
        FileReader input = null;
        LineNumberReader count = null;
        try {
            input = new FileReader(fileName);
            count = new LineNumberReader(input);
            while (count.skip(Long.MAX_VALUE) > 0) {
                // Loop just in case the file is > Long.MAX_VALUE or skip() decides to not read the entire file
            }
            return count.getLineNumber() - 1; // remove the header
        } catch (Exception e) {
            return 0;
        } finally {
            if (count != null)
                try {
                    count.close();
                } catch (IOException e) {

                }
            if (input != null)
                try {
                    input.close();
                } catch (IOException e) {

                }
        }
    }

    BufferedReader br;
    FileInputStream fr;
    String[] lineHeader = null;

    public void openCsvFile(String fileName) throws IOException {

        fr = new FileInputStream(importPath + File.separator + fileName);
        br = new BufferedReader(new InputStreamReader(fr, "UTF8"));

        String line = br.readLine();
        if (line != null)
            lineHeader = line.split(";");
    }

    public Map<String, Object> nextCsvRecord() throws IOException {
        String line = br.readLine();
        if (line == null)
            return null;

        String[] lineData = line.split(";");

        Map<String, Object> record = new HashMap<String, Object>();
        for (int i = 0; i < lineHeader.length; i++) {
            record.put(lineHeader[i], i < lineData.length ? decode(lineData[i]) : null);
        }
        return record;
    }

    public void closeCsvFile(String fileName, boolean allIsOk) {
        try {
            if (fr != null)
                fr.close();
            fr = null;
        } catch (Exception e) {
            logger.severe("Error during close file[" + importPath + File.separator + fileName + "]");
        }
        if (allIsOk) {
            // move the file to the importedPath
            if (importPath.compareTo(importedPath) == 0) {
                logger.severe("Imported path and import path are the same [" + importedPath + "]: do nothing");
            } else
                try {
                    Files.move(Paths.get(importPath + File.separator + fileName), Paths.get(importedPath + File.separator + fileName), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    logger.severe("Error during move file from[" + importPath + File.separator + fileName + "] to [" + importedPath + File.separator + fileName + "]");
                }
        }
    }

    public String decode(String source) {
        return source.replaceAll("%23", "#").replaceAll("%3B", ";");
    }

    public String encode(String source) {
        return source.replaceAll("#", "%23").replaceAll(";", "%3B");
    }

}
