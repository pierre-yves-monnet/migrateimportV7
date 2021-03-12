package org.bonitasoft.migrate;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.api.LoginAPI;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.util.APITypeManager;
import org.bonitasoft.migrate.repair.ImportRepair;
import org.bonitasoft.migrate.walker.ImportWalker;

import com.bonitasoft.engine.api.TenantAPIAccessor;

public class ImportV7 {

    static Logger logger = Logger.getLogger(ImportV7.class.getName());
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

    public boolean isDebug = false;

    public boolean historyInIndex = false;
    public int indexHistory = 2;
    public boolean historyInVariable = false;
    public boolean saveReportInCsv = false;

    public boolean onlyActiveCase = false;
    public boolean onlyArchivedCase = false;

    public int indexVerification = 1;
    // more than 70 characters

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
        usage += " -historyinindex <noindex|1|2|3|4|5> : save the history in StringIndex\n";
        usage += " -savereport : create a CSV file in the ouput path, one line per case\n";
        usage += " -onlyactivecase : Only active cases are imported, another are ignored\n";
        usage += " -onlyarchivedcase : Only archived cases are imported, another are ignored\n";
        usage += " -indexname <noindex|1|2|3|4|5> index name where the original case are saved to not reimport it;\n";
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
                String indexname = decodeArgs.nextArg();
                if ("noindex".equals(indexname))
                    importV7.historyInIndex = false;
                else {
                    try {
                        importV7.indexHistory = Integer.valueOf(indexname);
                        if (importV7.indexHistory < 1 || importV7.indexHistory > 5)
                            logger.severe("historyinindex must in <1|2|3|4|5> [" + indexname + "]. Usage : " + usage);
                        return;
                    } catch (Exception e) {
                        logger.severe("historyinindex must in <1|2|3|4|5> [" + indexname + "]. Usage : " + usage);
                        return;
                    }
                }

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
            } else if ("-indexname".equals(option)) {
                String indexname = decodeArgs.nextArg();
                if ("noindex".equals(indexname))
                    importV7.indexVerification = 0;
                else {
                    try {
                        importV7.indexVerification = Integer.valueOf(indexname);
                        if (importV7.indexVerification < 1 || importV7.indexVerification > 5)
                            logger.severe("indexname must in <noindex|1|2|3|4|5> [" + indexname + "]. Usage : " + usage);
                        return;
                    } catch (Exception e) {
                        logger.severe("indexname must in <noindex|1|2|3|4|5> [" + indexname + "]. Usage : " + usage);
                        return;
                    }
                }
            } else
                logger.severe("Unkown option [" + option + "]. Usage : " + usage);
            return;
        }

        importV7.applicationUrl = decodeArgs.nextArg();
        importV7.applicationName = decodeArgs.nextArg();
        importV7.userName = decodeArgs.nextArg();
        importV7.passwd = decodeArgs.nextArg();
        importV7.importPath = decodeArgs.nextArg();
        importV7.archivedPath = decodeArgs.nextArg();
        importV7.urlDatabase = decodeArgs.nextArg();
        if (importV7.archivedPath == null) {
            logger.severe(usage);
            // h2: jdbc:h2:file:D:/bonita/BPM-SP-7.7.3/workspace/Procergs-V5/h2_database/bonita_journal.db;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE;IGNORECASE=TRUE;AUTO_SERVER=TRUE;
            return;
        }
        logger.info("ImportV7 V1.4.0 (March 5 2019)");
        logger.info("java.version = [" + System.getProperty("java.version") + "]");

        if (!importV7.login(false)) {
            return;
        }

        
        
        // ------------------- CSV file
        ImportRepair importCsvFile = new ImportRepair() ;
        importCsvFile.importCsvFile(importV7);
        
        ImportWalker importJsonFile = new ImportWalker();
        importJsonFile.importJsonFile( importV7);
        
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
    public String applicationUrl;
    public String applicationName;
    public String userName;
    public String passwd;
    public String importPath;
    public String archivedPath;
    public String urlDatabase;

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
            logger.severe("Can't connect : " + e.getMessage());
            return false;
        }

    }

}
