package org.bonitasoft.migrate.walker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.migrate.ImportV7;
import org.bonitasoft.migrate.repair.FileCsv;
import org.bonitasoft.migrate.tool.OperationTimeTracker;
import org.bonitasoft.migrate.tool.Toolbox;
import org.bonitasoft.migrate.walker.RobotExecutor.ExecuteResult;

import com.bonitasoft.engine.api.ProcessAPI;
import com.bonitasoft.engine.api.TenantAPIAccessor;

public class ImportWalker {

    static Logger logger = Logger.getLogger(ImportWalker.class.getName());
    

    public void importJsonFile(ImportV7 importV7) {
        
        List<String> listFiles = FileCsv.getListFiles(importV7.importPath, ".json");

        // detect the number of line per process to calculed the total time needed
        int totalCase = listFiles.size();
        int totalErrors = 0;

        long begTime = System.currentTimeMillis();
        ProcessAPI processAPI;
        IdentityAPI identityAPI;
        try {
            processAPI = (com.bonitasoft.engine.api.ProcessAPI)TenantAPIAccessor.getProcessAPI(importV7.apiSession);
            identityAPI = TenantAPIAccessor.getIdentityAPI(importV7.apiSession);

        } catch (Exception e) {
            logger.severe(" Can't get ProcessAPI/IdentityAPI " + e.getMessage());
            return;
        }
        RobotExecutor robotExecutor = new RobotExecutor(this, importV7.apiSession, processAPI, identityAPI);

        for (String fileName : listFiles) {
            String logToFile = "";
            boolean isOk = true;
            try {
                RobotDataItem robotDataItem = new RobotDataItem();
                robotDataItem.readFromFile(importV7.importPath + File.separator + fileName);

                // Now execute it
                ExecuteResult executeResult = robotExecutor.execute(robotDataItem);
                isOk = executeResult.allIsCorrect;
                logToFile = executeResult.log.toString() + "\n" + executeResult.errorMessage.toString();
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                isOk = false;
                logToFile = "During load Exception " + e.getMessage() + " at " + exceptionDetails;
            }
            if (!isOk)
                totalErrors++;
            // write result in a file
            try (FileOutputStream fos = new FileOutputStream(importV7.archivedPath + File.separator + fileName + (isOk ? "_ok" : "_error"));
                    OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                    BufferedWriter writer = new BufferedWriter(osw)) {

                writer.append(logToFile);
                writer.flush();
                writer.close();
                
                
                Files.move(Paths.get(importV7.importPath + File.separator + fileName), Paths.get(importV7.archivedPath + File.separator + fileName), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                logger.severe("Error during move file from[" + importV7.importPath + File.separator + fileName + "] to [" + importV7.archivedPath + File.separator + fileName + "]");
            }

        }
        logger.info(" -- END Import JSON " + totalCase + " cases done in " + Toolbox.getHumanTime(System.currentTimeMillis() - begTime) + " ms Errors:" + totalErrors);
        logger.info(" Monitor " + getOperationTimeTracker().getOperationMarkerInfo());
    }

    /**
     * Keep an operation TimeTracker
     */
    private OperationTimeTracker operationTimeTracker = new OperationTimeTracker();

    public OperationTimeTracker getOperationTimeTracker() {
        return operationTimeTracker;
    }
}
