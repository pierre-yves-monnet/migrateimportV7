package org.bonitasoft.migrate.repair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.migrate.ImportV7;
import org.bonitasoft.migrate.tool.Toolbox;

public class ImportRepair {
    
    static Logger logger = Logger.getLogger(ImportRepair.class.getName());
    private static String blankLine = "                                                                                  ";


    public void importCsvFile( ImportV7 importV7 ) {
        MonitorImport monitorImport = new MonitorImport();
        List<String> listFiles = FileCsv.getListFiles(importV7.importPath, ".csv");

        // detect the number of line per process to calculed the total time needed
        int totalProcess = 0;
        int totalCase = 0;
        for (String fileName : listFiles) {
            long nbCases = FileCsv.getNumberCsvLine(importV7.importPath + File.separator + fileName);
            totalCase += nbCases;
            totalProcess++;
            logger.info("Detected [" + (fileName + "]" + blankLine).substring(0, 70) + " nbCases[" + nbCases + "]");
        }

        List<ProcessStatus> listWorkProcess = new ArrayList<>();

        for (String fileName : listFiles) {
            ProcessStatus processStatus = new ProcessStatus();
            processStatus.fileName = fileName;
            listWorkProcess.add(processStatus);
        }

        monitorImport.startImport(totalProcess, totalCase);

        for (ProcessStatus processStatus : listWorkProcess) {
            ImportProcessMgmt importProcessManagement = new ImportProcessMgmt();
            importProcessManagement.importOneProcess(importV7, processStatus, monitorImport, importV7.apiSession);
        }
        monitorImport.endImport();

        logger.info(" -- Status:");
        for (ProcessStatus processStatus : listWorkProcess)
            logger.info("   " + processStatus.toString());

        logger.info(" -- Errors:");
        int totalErrors = 0;
        for (ProcessStatus processStatus : listWorkProcess) {
            totalErrors += processStatus.listErrors.size();
            for (int i = 0; i < processStatus.listErrors.size(); i++)
                logger.info("   " + i + ". " + processStatus.listErrors.get(i));
        }
        logger.info(" -- END Import REPAIR/CSV" + monitorImport.totalProcess + " (" + monitorImport.totalCase + " cases) done in " + Toolbox.getHumanTime(monitorImport.getTotalTime()) + " Errors:" + totalErrors);
        logger.info(" Monitor " + monitorImport.getOperationTimeTracker().getOperationMarkerInfo());

    }
        
}
