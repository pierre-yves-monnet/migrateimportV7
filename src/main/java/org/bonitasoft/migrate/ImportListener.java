package org.bonitasoft.migrate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.migrate.ImportProcessMgmt.ProcessInstanceContext;
import org.bonitasoft.migrate.ImportProcessMgmt.StatusImport;

public class ImportListener {

  static Logger logger = Logger.getLogger(ImportListener.class.getName());

  private FileOutputStream fileReport = null;
  private BufferedWriter bwReport;

  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
  SimpleDateFormat sdfFile = new SimpleDateFormat("yyyyMMddHHmmss");
  List<String> listHeader = new ArrayList<String>();
  int countLine = 0;
  String fileName;

  public void startListener(boolean saveInCsv, String processName, String processVersion, String prefixReportName, String importedPath) {
    if (!saveInCsv)
      return;

    listHeader.add("Status");
    listHeader.add("CaseIDV5");
    listHeader.add("CaseIDV7");
    listHeader.add("History");
    fileName = importedPath + File.separatorChar + processName + "_" + processVersion + "_" + sdfFile.format(new Date()) + prefixReportName + ".csv";
    try {
      fileReport = new FileOutputStream(fileName);

      bwReport = new BufferedWriter(new OutputStreamWriter(fileReport, "UTF8"));
      countLine = 0;

    } catch (Exception e) {
      logger.severe("During create ReportFile[" + fileName + "] " + e.getMessage());
    }

  }

  public void caseStatus(StatusImport statusImport, ProcessInstanceContext processInstanceContext) {
    if (fileReport == null)
      return;
    // create a line with Case ID in V5;Case ID in V7;exercicio;cod_int_processo
    try {

      Map<String, Object> rootProcessMap = processInstanceContext.getRootVariableProcess();
      if (countLine == 0) {
        // calculate the Header
        if (rootProcessMap != null)
          for (String variableName : rootProcessMap.keySet()) {
            listHeader.add(variableName);
          }

        bwReport.write(getLine(listHeader, null));
        bwReport.newLine();

      }
      countLine++;
      Map<String, Object> contentReport = new HashMap<String, Object>();
      if (rootProcessMap != null)
        contentReport.putAll(rootProcessMap);

      contentReport.put("CaseIDV5", processInstanceContext.caseIdV5);
      contentReport.put("CaseIDV7", processInstanceContext.caseIdV7);
      contentReport.put("Status", statusImport.toString());
      contentReport.put("History", processInstanceContext.historyCase);
      
      bwReport.write(getLine(listHeader, contentReport));
      bwReport.newLine();
      if (countLine % 500 == 0) {
        bwReport.flush();
        fileReport.flush();
      }
    } catch (IOException e) {
      logger.severe("During create ReportFile[" + fileName + "] " + e.getMessage());
    }

  }

  public void endListener() {
    if (fileReport == null)
      return;

    try {
      bwReport.flush();
      fileReport.close();
    } catch (IOException e) {
      logger.severe("During create ReportFile[" + fileName + "] " + e.getMessage());
    }

  }

  private String getLine(List<String> header, Map<String, Object> content) {
    StringBuffer result = new StringBuffer();
    for (int i = 0; i < header.size(); i++) {
      if (i > 0)
        result.append(";");
      // content is null ==> Print the header itself
      if (content == null)
        result.append(header.get(i));
      else {
        Object o = content.get(header.get(i));
        if (o == null)
          result.append("");
        else if (o instanceof Date)
          result.append(sdf.format((Date) o));
        else
          result.append(encode(o.toString()));
      }

    }
    return result.toString();
  }

  public String encode(String source) {
    return source.replaceAll("#", "%23").replaceAll(";", "%3B");
  }

}
