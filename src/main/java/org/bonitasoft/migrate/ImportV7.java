package org.bonitasoft.migrate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardCopyOption.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import javax.security.auth.login.LoginException;

import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.LoginAPI;
import org.bonitasoft.engine.bpm.flownode.EndEventDefinition;
import org.bonitasoft.engine.bpm.flownode.FlowElementContainerDefinition;
import org.bonitasoft.engine.bpm.flownode.GatewayDefinition;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.DesignProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.expression.ExpressionBuilder;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.operation.Operation;
import org.bonitasoft.engine.operation.OperationBuilder;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.util.APITypeManager;

import com.bonitasoft.engine.api.TenantAPIAccessor;
import com.bonitasoft.engine.api.ProcessAPI;
import com.bonitasoft.engine.bpm.process.Index;
import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceSearchDescriptor;

import org.bonitasoft.engine.session.APISession;

import org.json.simple.JSONValue;

public class ImportV7 {

  static Logger logger = Logger.getLogger(ImportV7.class.getName());
  static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

  public boolean isDebug = false;

  public boolean historyInIndex = false;
  public boolean historyInVariable = false;
  public boolean saveReportInCsv = false;

  public boolean onlyActiveCase = false;
  public boolean onlyArchivedCase = false;

  /**
   * Decode the different args
   */
  private static class DecodeArg {

    String[] args;
    int indexArgs = 0;

    public DecodeArg(final String[] args) {
      this.args = args;
      this.indexArgs = 0;
    }

    public String nextArg() {
      if (indexArgs >= args.length)
        return null;
      indexArgs++;
      return args[indexArgs - 1];
    }

    public boolean isNextArgsAndOption() {
      if (indexArgs >= args.length)
        return false;
      if (args[indexArgs].startsWith("-"))
        return true;
      return false;

    }
  }

  // PROCERGS_FEI_SERVICO_MOVEL_DEVOLUCAO : OK
  /**
   * @param args
   */
  public static void main(final String[] args) {

    ImportV7 importV7 = new ImportV7();

    // String providerURL          = args.length>0 ? args[0] : null;
    DecodeArg decodeArgs = new DecodeArg(args);
    List<String> listProcessFilter = new ArrayList<String>();

    String usage = "[option] APPLICATIONURL APPLICATIONNAME LOGINNAME PASSWORD IMPORTPATH MOVEIMPORTEDPATH URLDATABASE\n";
    usage += "  Options: -f <ListOfProcesses> import only theses process, example -f CIENTEC_ASC_AssinaturaLaudos--1.0;DOC___Publicacao_de_documento--3.0\n";
    usage += " -debug : more information on log\n";
    usage += " -historyinindex : save the history in StringIndex 2\n";
    usage += " -savereport : create a CSV file in the ouput path, one line per case\n";
    usage += " -onlyactivecase : Only active cases are imported, another are ignored\n";
    usage += " -onlyarchivedcase : Only archived cases are imported, another are ignored\n";
    usage += "  Example -f CIENTEC_ASC_AssinaturaLaudos--1.0;DOC___Publicacao_de_documento--3.0 http://localhost:8080 bonita walter.bates bmp c:/importBonita c:/importBonita/FilesImported jdbc:postgresql://localhost:3412/test?user=fred&password=secret";
    while (decodeArgs.isNextArgsAndOption()) {
      String option = decodeArgs.nextArg();
      if ("-f".equals(option)) {
        String listProcessesSt = decodeArgs.nextArg();
        StringTokenizer st = new StringTokenizer(listProcessesSt, ";");
        while (st.hasMoreTokens()) {
          listProcessFilter.add(st.nextToken());
        }
      } else if ("-debug".equals(option)) {
        importV7.isDebug = true;
      } else if ("-historyinindex".equals(option)) {
        importV7.historyInIndex = true;
      }
      //else if ("-historyinvariable".equals(option)) {
      //  importV7.historyInVariable=true;
      //}
      else if ("-savereport".equals(option)) {
        importV7.saveReportInCsv = true;
      } else if ("-onlyactivecase".equals(option)) {
        importV7.onlyActiveCase = true;
      } else if ("-onlyarchivedcase".equals(option)) {
        importV7.onlyArchivedCase = true;
      } else
        logger.severe("Unkown option [" + option + "]. Usage : " + usage);

    }

    importV7.applicationUrl = decodeArgs.nextArg();
    importV7.applicationName = decodeArgs.nextArg();
    importV7.userName = decodeArgs.nextArg();
    importV7.passwd = decodeArgs.nextArg();
    importV7.importPath = decodeArgs.nextArg();
    importV7.importedPath = decodeArgs.nextArg();
    importV7.urlDatabase = decodeArgs.nextArg();
    if (importV7.importedPath == null) {
      logger.severe(usage);
      // h2: jdbc:h2:file:D:/bonita/BPM-SP-7.7.3/workspace/Procergs-V5/h2_database/bonita_journal.db;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE;IGNORECASE=TRUE;AUTO_SERVER=TRUE;
      return;
    }
    logger.info("ImportV7 V1.4.0 (March 5 2019)");
    logger.info("java.version = [" + System.getProperty("java.version") + "]");

    importV7.login(false);

    MonitorImport monitorImport = new MonitorImport();
    List<String> listFiles = FileCsv.getListFiles(importV7.importPath);

    // detect the number of line per process to calculed the total time needed
    int totalProcess = 0;
    int totalCase = 0;
    for (String fileName : listFiles) {
      long nbCases = FileCsv.getNumberCsvLine(importV7.importPath + File.separator + fileName);
      totalCase += nbCases;
      totalProcess++;
      logger.info("Detected [" + (fileName + "]                                               ").substring(0, 70) + " nbCases[" + nbCases + "]");
    }

    List<ProcessStatus> listWorkProcess = new ArrayList<ProcessStatus>();

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
    logger.info(" -- END Import " + monitorImport.totalProcess + " (" + monitorImport.totalCase + " cases) done in " + ProcessStatus.getHumanTime(monitorImport.getTotalTime()) + " Errors:" + totalErrors);

  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /*
   * Login
   */
  /* ******************************************************************************** */
  /**
   * login
   */
  String applicationUrl;
  String applicationName;
  String userName;
  String passwd;
  String importPath;
  String importedPath;
  String urlDatabase;

  public APISession apiSession;

  public boolean login(final boolean newConnectMethod) {
    try {
      // Define the REST parameters
      final Map<String, String> map = new HashMap<String, String>();

      if (applicationUrl == null)
        applicationUrl = "http://localhost:7080";
      if (applicationName == null)
        applicationName = "bonita";

      if (newConnectMethod) {
        map.put("org.bonitasoft.engine.api-type.server.url", applicationUrl);
        map.put("org.bonitasoft.engine.api-type.application.name", applicationName);
      } else {
        map.put("server.url", applicationUrl);
        map.put("application.name", applicationName);
      }
      APITypeManager.setAPITypeAndParams(ApiAccessType.HTTP, map);
      logger.info("Login to [" + applicationUrl + "] / [" + applicationName + "] login[" + userName + "]/[" + passwd + "]");
      // Set the username and password
      // final String username = "helen.kelly";

      // get the LoginAPI using the TenantAPIAccessor
      LoginAPI mLoginAPI = TenantAPIAccessor.getLoginAPI();

      // log in to the tenant to create a session
      apiSession = mLoginAPI.login(userName, passwd);

      return true;
    } catch (final Exception e) {
      logger.severe("Error " + e.getMessage());
      return false;
    }

  }

}
