package org.bonitasoft.migrate;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.management.MonitorInfo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.bpm.actor.ActorInstance;
import org.bonitasoft.engine.bpm.data.DataInstance;
import org.bonitasoft.engine.bpm.flownode.ActivityDefinition;
import org.bonitasoft.engine.bpm.flownode.ActivityInstance;
import org.bonitasoft.engine.bpm.flownode.ActivityStates;
import org.bonitasoft.engine.bpm.flownode.EndEventDefinition;
import org.bonitasoft.engine.bpm.flownode.FlowElementContainerDefinition;
import org.bonitasoft.engine.bpm.flownode.FlowNodeDefinition;
import org.bonitasoft.engine.bpm.flownode.FlowNodeInstance;
import org.bonitasoft.engine.bpm.flownode.GatewayDefinition;
import org.bonitasoft.engine.bpm.flownode.HumanTaskDefinition;
import org.bonitasoft.engine.bpm.flownode.TaskInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.DesignProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceState;
import org.bonitasoft.engine.bpm.userfilter.UserFilterDefinition;
import org.bonitasoft.engine.expression.ExpressionBuilder;
import org.bonitasoft.engine.expression.ExpressionType;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.operation.Operation;
import org.bonitasoft.engine.operation.OperationBuilder;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.migrate.MonitorImport.OperationMarkerToken;
import org.bonitasoft.migrate.ProcessInstanceDescription.TasksInstanceIndentified;
import org.json.simple.JSONValue;

import com.bonitasoft.engine.api.ProcessAPI;
import com.bonitasoft.engine.api.TenantAPIAccessor;
import com.bonitasoft.engine.bpm.process.Index;
import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceSearchDescriptor;
import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceUpdater;

public class ImportProcessMgmt {

    static Logger logger = Logger.getLogger(ImportV7.class.getName());

    static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
    static SimpleDateFormat sdfHistory = new SimpleDateFormat("dd/MM/yyyy HH:mm");

    ProcessAPI processAPI;
    IdentityAPI identityAPI;
    CommandAPI commandAPI;
    APISession apiSession;
    Map<String, Long> cacheUserName = new HashMap<String, Long>();
    Map<String, GatewayDefinition> mapGatewaysDefinition;

    PlaySql playSql;

    ProcessStatus processStatus;
    MonitorImport monitorImport;

    /**
     * V5 export process
     * return the number of case exported
     */
    public int importOneProcess(ImportV7 importV7, ProcessStatus processStatus, MonitorImport monitorImport, APISession apiSession) {

        // search the process
        this.processStatus = processStatus;
        this.monitorImport = monitorImport;
        this.apiSession = apiSession;
        this.playSql = new PlaySql(importV7.urlDatabase);

        FileCsv fileCsv = new FileCsv(importV7.importPath, importV7.importedPath);
        long beginProcess = System.currentTimeMillis();

        ImportListener importListener = new ImportListener();
        try {
            playSql.openPlaySql();
            processStatus.nbCasesDetected = (int) FileCsv.getNumberCsvLine(importV7.importPath + File.separator + processStatus.fileName);

            // --------- preparation
            processAPI = (com.bonitasoft.engine.api.ProcessAPI) TenantAPIAccessor.getProcessAPI(apiSession);
            identityAPI = TenantAPIAccessor.getIdentityAPI(apiSession);
            commandAPI = TenantAPIAccessor.getCommandAPI(apiSession);
            int pos = processStatus.fileName.lastIndexOf("--");
            if (pos == -1) {
                logger.info("No [--] in the file. Expected <processName>--<Version> - can't decode the file name");
                processStatus.error("No [--] in the file - can't decode the file name [" + processStatus.fileName + "]", null);

                return 0;
            }
            processStatus.processName = processStatus.fileName.substring(0, pos);
            processStatus.processVersion = processStatus.fileName.substring(pos + 2);
            if (processStatus.processVersion.length() > 4)
                processStatus.processVersion = processStatus.processVersion.substring(0, processStatus.processVersion.length() - 4);

            String prefixReportName = "";
            if (importV7.onlyActiveCase)
                prefixReportName += "_ACTIVE";
            if (importV7.onlyArchivedCase)
                prefixReportName += "_ARCHIVED";

            importListener.startListener(importV7.saveReportInCsv, processStatus.processName, processStatus.processVersion, prefixReportName, importV7.importedPath);

            long processDefinitionId = getPermissiveProcessDefinition(processStatus.processName, processStatus.processVersion, processAPI);

            processStatus.processDefinition = processAPI.getProcessDefinition(processDefinitionId);
            //------------ search the END activity : necessary to end it

            DesignProcessDefinition designProcessDefinition = processAPI.getDesignProcessDefinition(processStatus.processDefinition.getId());
            FlowElementContainerDefinition flowElement = designProcessDefinition.getFlowElementContainer();
            List<EndEventDefinition> listEndEvents = flowElement.getEndEvents();
            for (EndEventDefinition endEventDefinition : listEndEvents) {
                // we need a simple end event, not a endevent which send a signal or send a message
                if (endEventDefinition.getMessageEventTriggerDefinitions().isEmpty()
                        && endEventDefinition.getSignalEventTriggerDefinitions().isEmpty())
                    processStatus.endEventName = endEventDefinition.getName();
            }

            // searh a human task
            List<ActivityDefinition> listActivities = flowElement.getActivities();
            for (ActivityDefinition activity : listActivities) {
                // we need a simple end event, not a endevent which send a signal or send a message
                if (activity instanceof HumanTaskDefinition)
                    processStatus.oneHumanTask = activity.getName();
            }

            mapGatewaysDefinition = new HashMap<String, GatewayDefinition>();
            @SuppressWarnings("deprecation")
            Set<GatewayDefinition> listGateways = flowElement.getGateways();
            for (GatewayDefinition gateway : listGateways) {
                // we need a simple end event, not a endevent which send a signal or send a message
                mapGatewaysDefinition.put(gateway.getName(), gateway);
            }

            //---------------- load the file
            logger.info("Load file [" + processStatus.fileName + "]");
            fileCsv.openCsvFile(processStatus.fileName);
            Map<String, Object> record;

            // loop
            while ((record = fileCsv.nextCsvRecord()) != null) {

                if ((processStatus.getNbCasesProcessed() % 500) == 499) {

                    // calcul the total time
                    double percentAdv = processStatus.getNbCasesProcessed() == 0 ? 100 : (double) (100.0 * ((double) processStatus.getNbCasesProcessed()) / ((double) processStatus.nbCasesDetected));
                    long durationAdv = System.currentTimeMillis() - beginProcess;
                    long durationTotal = percentAdv == 0 ? 0 : (long) (100.0 * durationAdv / percentAdv);

                    logger.info("    MonitorExport:" + monitorImport.getOperationMarkerInfo());
                    logger.info(processStatus.processName + "  " + processStatus.getNbCasesProcessed() + "/" + processStatus.nbCasesDetected + " (" + String.format("%.1f", percentAdv) + " %)" + (percentAdv > 0 ? (" ~> " + ProcessStatus.getHumanTime(durationTotal - durationAdv)) : "") + " GLOBAL:"
                            + monitorImport.getEstimationToEnd(processStatus.getNbCasesProcessed(), processStatus.nbCaseImported, processStatus.nbCaseAlreadyPresent, processStatus.nbCaseError));
                }

                OperationMarkerToken tokenOneCase = monitorImport.startOperationMarker("case");
                StatusImport statusImport = importOneProcessInstance(record, importV7, importListener);
                monitorImport.endOperationMarker(tokenOneCase);

                if (statusImport == StatusImport.OK)
                    processStatus.nbCaseImported++;
                if (statusImport == StatusImport.ALREADYIMPORTED)
                    processStatus.nbCaseAlreadyPresent++;
                if (statusImport == StatusImport.ERRORIMPORT)
                    processStatus.nbCaseError++;

            } // next record
            fileCsv.closeCsvFile(processStatus.fileName, true);

        } catch (ProcessDefinitionNotFoundException pe) {
            processStatus.error("Can't find process [" + processStatus.processName + "] [" + processStatus.processVersion + "] fileName=[" + processStatus.fileName + "]", null);
            fileCsv.closeCsvFile(processStatus.fileName, false);
        } catch (Exception e) {
            processStatus.error("Can't import case a case : " + e.getMessage(), null);
            fileCsv.closeCsvFile(processStatus.fileName, false);
        }
        playSql.closePlaySql();
        if (importListener != null)
            importListener.endListener();
        long endProcess = System.currentTimeMillis();

        monitorImport.addStepProcess(processStatus.nbCaseImported + processStatus.nbCaseError + processStatus.nbCaseAlreadyPresent);
        processStatus.timeExecution = endProcess - beginProcess;
        logger.info(processStatus.fileName + "  Imported:" + processStatus.nbCaseImported + ", Present:" + processStatus.nbCaseAlreadyPresent + " Error:" + processStatus.nbCaseError + " Detected:" + processStatus.nbCasesDetected + " Finished in " + ProcessStatus.getHumanTime(endProcess - beginProcess));

        return processStatus.nbCaseImported;
    }

    // IGNORE : this case has to be ignored (onlyactive case for example)
    public enum StatusImport {
        OK, ALREADYIMPORTED, ERRORIMPORT, IGNORE
    };

    private enum OperationCreation {
        TASKS, ONEHUMANTASKTHENARCHIVE, ENDPOINT
    };

    /**
     * keep the context of one case imported
     */
    public static class ProcessInstanceContext {

        public String caseIdV5;
        public Long caseIdV7;
        public boolean isActiveCase;
        public StringBuilder historyCase = new StringBuilder();
        ProcessStatus processStatus;
        ProcessInstance rootProcessInstance = null;
        ProcessInstanceDescription processInstanceDescription = new ProcessInstanceDescription();
        boolean foundTaskInError = false;

        private String collectErrorMessage = "";
        public VariablesDescription variablesProcesses = null;
        public VariablesDescription variablesActivity = null;

        Set<String> tasksInErrorBeforeMigration = new HashSet<String>();

        Map<String, Object> record;
        boolean isDebug;

        ProcessInstanceContext(Map<String, Object> record, boolean isDebug) {
            this.record = record;
            this.isDebug = isDebug;

        }

        /**
         * add a message in the error, does not report it now
         * 
         * @param message
         */
        public void addErrorMessage(String message) {
            collectErrorMessage += message + ";";
        }

        public void error(String message) {
            String labelError = "CaseIdV5[" + caseIdV5 + "] Active? " + isActiveCase + " " + message;
            if (isDebug)
                labelError += "CSV Record=" + record.toString();
            processStatus.error(labelError, rootProcessInstance);
        }

        public Map<String, Object> getRootVariableProcess() {
            if (variablesProcesses == null)
                return null;
            return variablesProcesses.getRootProcess(processStatus.processName, processStatus.processVersion);
        }
    }

    /**
     * @param record
     * @param processStatus
     * @param monitorImport
     * @param cacheUserName
     * @param mapGatewaysDefinition
     * @param processAPI
     * @param identityAPI
     * @param commandAPI
     * @return
     */
    public StatusImport importOneProcessInstance(Map<String, Object> record, ImportV7 importV7, ImportListener importListener) {
        boolean reportExceptionError = true;
        ProcessInstanceContext processInstanceContext = new ProcessInstanceContext(record, importV7.isDebug);
        processInstanceContext.processStatus = processStatus;

        String typeLine = (String) record.get(FileCsv.TYPELINE);
        processInstanceContext.isActiveCase = typeLine.equals(FileCsv.TYPECASEACTIVE);
        processInstanceContext.caseIdV5 = (String) record.get(FileCsv.CASEID);

        if (importV7.onlyActiveCase && !processInstanceContext.isActiveCase)
            return StatusImport.IGNORE;
        if (importV7.onlyArchivedCase && processInstanceContext.isActiveCase)
            return StatusImport.IGNORE;

        try {

            boolean isAlreadyPresent = false;
            if (importV7.indexVerification == 0) {
                // accept it
                isAlreadyPresent = false;
            } else {
                // is this caseId already loaded ?
                SearchOptionsBuilder searchOptionsBuilder = new SearchOptionsBuilder(0, 100);
                String filterField = getIndexFilterFromNumber(importV7.indexVerification);
                searchOptionsBuilder.filter(filterField, processInstanceContext.caseIdV5);

                searchOptionsBuilder.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processStatus.processDefinition.getId());

                if (processInstanceContext.isActiveCase) {

                    OperationMarkerToken tokenSearchProcessInstances = monitorImport.startOperationMarker("searchProcessInstances");
                    SearchResult<ProcessInstance> searchProcessInstance = processAPI.searchProcessInstances(searchOptionsBuilder.done());
                    monitorImport.endOperationMarker(tokenSearchProcessInstances);

                    isAlreadyPresent = (!searchProcessInstance.getResult().isEmpty());
                    if (isAlreadyPresent)
                        processInstanceContext.caseIdV7 = searchProcessInstance.getResult().get(0).getRootProcessInstanceId();
                } else {
                    OperationMarkerToken tokenSearchProcessInstances = monitorImport.startOperationMarker("searchProcessInstances");
                    SearchResult<ArchivedProcessInstance> searchProcessInstance = processAPI.searchArchivedProcessInstances(searchOptionsBuilder.done());
                    monitorImport.endOperationMarker(tokenSearchProcessInstances);

                    isAlreadyPresent = (!searchProcessInstance.getResult().isEmpty());
                    if (isAlreadyPresent)
                        processInstanceContext.caseIdV7 = searchProcessInstance.getResult().get(0).getRootProcessInstanceId();
                }
            }

            processInstanceContext.variablesProcesses = decodeVariables((String) record.get(FileCsv.VARIABLES), CONTEXT_DECODAGE.PROCESSVARIABLE);
            processInstanceContext.variablesActivity = decodeVariables((String) record.get(FileCsv.VARIABLESACTIVITY), CONTEXT_DECODAGE.TASKVARIABLE);

            OperationCreation operationCreation;

            boolean operationUpdateVariable = false;
            boolean operationAtBegining = false;
            boolean operationUpdateIndex = false;
            boolean operationUpdateIndexBySQL = false;
            if (processInstanceContext.isActiveCase) {
                operationCreation = OperationCreation.TASKS;
                operationUpdateVariable = true;
                operationUpdateIndex = true;
                operationAtBegining = false;
            } else if (processStatus.oneHumanTask != null) {
                operationCreation = OperationCreation.ONEHUMANTASKTHENARCHIVE;
                operationUpdateVariable = true;
                operationUpdateIndex = true;
                operationAtBegining = false;
            } else {
                operationCreation = OperationCreation.ENDPOINT;
                operationUpdateVariable = false;
                operationUpdateIndex = false;
                operationAtBegining = true;
                operationUpdateIndexBySQL = true;
            }
            final Map<String, Serializable> parameters = new HashMap<String, Serializable>();
            parameters.put("process_definition_id", processStatus.processDefinition.getId());

            List<TaskDescription> listTaskIdentifier = null;
            ArrayList<String> startPoints = new ArrayList<String>();

            // -------------------- calculate the list of history
            String tasks = (String) record.get(FileCsv.TASKS);
            // task format is Parent--1.0--1--Etape grandChild--READY#Parent--1.0--1--call grandChild--EXECUTING
            if (tasks.trim().length() == 0)
                processInstanceContext.error("Case does not have tasks in CSV");

            listTaskIdentifier = decodeListTaskIdentifier(tasks);
            if (listTaskIdentifier.isEmpty())
                processInstanceContext.error("Task can't be decoded from [" + tasks + "]");

            // if the task is a Multi iteration, we will have the same taskName multiple time -- does not manage it correclty at this moment
            for (TaskDescription taskIdentifier : listTaskIdentifier) {
                if (taskIdentifier.error != null) {
                    processInstanceContext.error("Error decoding task:" + taskIdentifier.error);
                    continue;
                }
                // an executing task (call activity) must not be started, it will be done automaticaly
                if (taskIdentifier.contextInformation.equals("READY") || taskIdentifier.contextInformation.equals("FAILED"))
                    startPoints.add(taskIdentifier.taskName);
                // LOOP Activity: the main task may be EXECUTING
                if (taskIdentifier.contextInformation.equals("EXECUTING"))
                    startPoints.add(taskIdentifier.taskName);
                // Archive task
                // if (taskIdentifier.contextInformation.equals("FINISHED"))
                //  startPoints.add(taskIdentifier.taskName);
                if (taskIdentifier.contextInformation.equals("FAILED"))
                    processInstanceContext.tasksInErrorBeforeMigration.add(taskIdentifier.taskName);
            }

            // -------------------- calculate history
            String startedBy = (String) record.get(FileCsv.STARTEDBY);
            String startedWhenSt = (String) record.get(FileCsv.STARTEDDATE);
            Date startedWhen = null;
            try {
                startedWhen = sdf.parse(startedWhenSt);
            } catch (Exception e) {
            }

            processInstanceContext.historyCase.append("STARTED: " + (startedWhen == null ? "" : sdfHistory.format(startedWhen)) + "(" + startedBy + ")");
            // who started it?

            if (listTaskIdentifier != null) {
                for (TaskDescription taskIdentifier : listTaskIdentifier) {
                    if (taskIdentifier.contextInformation.equals("FINISHED"))
                        processInstanceContext.historyCase.append("," + taskIdentifier.taskName + ": " + sdfHistory.format(taskIdentifier.dateExecution) + "(" + taskIdentifier.executedBy + ")");
                }
            }
            // -------------------- Time to skip now
            if (isAlreadyPresent) {
                // already imported
                importListener.caseStatus(StatusImport.ALREADYIMPORTED, processInstanceContext);
                return StatusImport.ALREADYIMPORTED;
            }

            // ---------------------- prepare the creation 

            if (operationCreation == OperationCreation.TASKS) {

                if (startPoints.isEmpty()) {
                    processInstanceContext.error(OperationCreation.TASKS.toString() + ": Case does not have active task by" + FileCsv.TASKS + "=[" + tasks + "] ");
                    importListener.caseStatus(StatusImport.ERRORIMPORT, processInstanceContext);

                    return StatusImport.ERRORIMPORT;
                }

            } else if (operationCreation == OperationCreation.ONEHUMANTASKTHENARCHIVE) {
                // set as started a human task item
                startPoints.add(processStatus.oneHumanTask);
                // set as started a END item
                startPoints.add(processStatus.endEventName);
            }
            parameters.put("activity_names", startPoints);

            // start by

            if (cacheUserName.containsKey(startedBy))
                parameters.put("started_by", cacheUserName.get(startedBy));
            else {
                try {
                    User user = identityAPI.getUserByUserName(startedBy);
                    cacheUserName.put(startedBy, user.getId());
                    parameters.put("started_by", user.getId());
                } catch (Exception e) {
                    parameters.put("started_by", apiSession.getUserId());
                }

            }

            // variables
            // update the data now

            List<Operation> listOperations = new ArrayList<Operation>();
            if (operationAtBegining) {
                // only way for the Archive case : go by the operation, and we collect only the ROOT process task
                Map<String, Object> rootVariables = processInstanceContext.variablesProcesses.getRootProcess(processStatus.processDefinition.getName(), processStatus.processDefinition.getVersion());
                for (String key : rootVariables.keySet()) {
                    //set variable myBooleanVar
                    Object value = rootVariables.get(key);
                    Operation op = null;
                    if (value == null)
                        continue;
                    if (value instanceof Boolean)
                        op = new OperationBuilder().createSetDataOperation(key, new ExpressionBuilder().createConstantBooleanExpression((Boolean) value));

                    if (value instanceof Integer)
                        op = new OperationBuilder().createSetDataOperation(key, new ExpressionBuilder().createConstantIntegerExpression((Integer) value));

                    if (value instanceof Long)
                        op = new OperationBuilder().createSetDataOperation(key, new ExpressionBuilder().createConstantLongExpression((Long) value));

                    if (value instanceof Date) {
                        String groovyForDate = "import java.text.SimpleDateFormat;\n"
                                + " SimpleDateFormat sdf = new SimpleDateFormat(\"yyyyMMdd HH:mm:ss\");\n"
                                + " return sdf.parse(\"" + sdf.format((Date) value) + "\");";
                        // op = new OperationBuilder().createSetDataOperation(key, new ExpressionBuilder().createExpression("GenerateDate", groovyForDate, Date.class.getName(), ExpressionType.TYPE_VARIABLE).setInterpreter());
                        ExpressionBuilder expressionBuilder = new ExpressionBuilder();
                        expressionBuilder = expressionBuilder.createNewInstance("GenerateDate").setContent(groovyForDate).setExpressionType(ExpressionType.TYPE_READ_ONLY_SCRIPT).setReturnType(Date.class.getName()).setInterpreter("GROOVY");
                        op = new OperationBuilder().createSetDataOperation(key, expressionBuilder.done());
                    }
                    if (value instanceof String)
                        op = new OperationBuilder().createSetDataOperation(key, new ExpressionBuilder().createConstantStringExpression((String) value));

                    if (value instanceof List) {
                        String groovyForList = "return [";
                        @SuppressWarnings("unchecked")
                        List<Object> valueList = (List<Object>) value;
                        for (int i = 0; i < valueList.size(); i++) {
                            if (i > 0)
                                groovyForList += ", ";
                            groovyForList += " \"" + valueList.get(i).toString() + "\"";
                        }
                        groovyForList += "];";
                        ExpressionBuilder expressionBuilder = new ExpressionBuilder();
                        expressionBuilder = expressionBuilder.createNewInstance("GenerateList").setContent(groovyForList).setExpressionType(ExpressionType.TYPE_READ_ONLY_SCRIPT).setReturnType(List.class.getName()).setInterpreter("GROOVY");
                        op = new OperationBuilder().createSetDataOperation(key, expressionBuilder.done());

                    }

                    if (op != null)
                        listOperations.add(op);
                    else
                        processStatus.error("This data [" + value.getClass().getName() + "] is not supported for an Archived process :", null);
                }

            }
            parameters.put("operations", (Serializable) listOperations);

            // document
            final HashMap<String, Serializable> context = new HashMap<String, Serializable>();
            parameters.put("context", context);

            // -------------------------------------------- Create the case now
            OperationMarkerToken tokenCommandExecute = monitorImport.startOperationMarker("commandExecute");
            Serializable resultCommand = commandAPI.execute("multipleStartPointsProcessCommand", parameters);
            monitorImport.endOperationMarker(tokenCommandExecute);

            if (!(resultCommand instanceof ProcessInstance)) {
                processStatus.error("Can't create a case, result is [" + resultCommand.getClass().getName() + "]", null);
                importListener.caseStatus(StatusImport.ERRORIMPORT, processInstanceContext);
                return StatusImport.ERRORIMPORT;
            }

            processInstanceContext.rootProcessInstance = (ProcessInstance) resultCommand;
            processInstanceContext.caseIdV7 = processInstanceContext.rootProcessInstance.getRootProcessInstanceId();

            // ---------------------- continue the import now : PROCESS and TASK variable 
            if (operationUpdateVariable) {
                // first, we have to find ALL process involved in the new RootProcessinstance, in order to reattache the variable correclty
                processInstanceContext.processInstanceDescription.load(processInstanceContext.rootProcessInstance.getId(), processAPI, monitorImport);

                // don't manage the Call Activity multiple, search the first item, that's all.
                // if we need to do that, 1/ we have the V5 Case Id in operation, and the V5 CaseId in the variable, so it's possible to rebuild a link
                String taskExecutionMessage = "";
                updateVariableProcess(processInstanceContext);

                // first pass, do not log the "task is not ready"
                if (!updateVariableTasks(processInstanceContext, false))
                    processInstanceContext.foundTaskInError = true;

                // Special case : we may have at this point some task Failed, due to some actor filter based on the variable. Then replay the task, to try to save this case.
                // if the case is an ARCHIVED one, doesn't matter, all tasks will be move to archived
                if (processInstanceContext.isActiveCase) {
                    for (FlowNodeInstance flowNodeInstance : processInstanceContext.processInstanceDescription.searchFlowNode.getResult()) {
                        if (flowNodeInstance.getState().equals(ActivityStates.FAILED_STATE)) {
                            if (processInstanceContext.tasksInErrorBeforeMigration.contains(flowNodeInstance.getName())) {
                                // task was in error before, so it's normal, don't try to do anything
                                // maybe it was set to true by the first update
                                processInstanceContext.foundTaskInError = false;
                            } else {
                                processInstanceContext.foundTaskInError = true;
                                try {
                                    processAPI.retryTask(flowNodeInstance.getId());
                                } catch (Exception e) {
                                    taskExecutionMessage += "Task[" + flowNodeInstance.getName() + "] still in error " + e.getMessage();
                                }
                            }
                        }
                    }
                }
                if (taskExecutionMessage.length() > 0)
                    processInstanceContext.error("Task still failed :" + taskExecutionMessage);

                // try to update again the task
                if (processInstanceContext.foundTaskInError)
                    updateVariableTasks(processInstanceContext, true);

                if (processInstanceContext.collectErrorMessage.length() > 0) {
                    processInstanceContext.error(processInstanceContext.collectErrorMessage);
                    reportExceptionError = false;
                    throw new Exception(processInstanceContext.collectErrorMessage);
                }

            } // end doUpdateVariables

            // update the String Index
            if (operationUpdateIndex) {
                Index index = getIndexFromNumber(importV7.indexVerification);
                if (index != null) {
                    OperationMarkerToken tokenUpdateProcessInstanceIndex = monitorImport.startOperationMarker("updateProcessInstanceIndex");
                    processAPI.updateProcessInstanceIndex(processInstanceContext.rootProcessInstance.getId(), index, (String) record.get(FileCsv.CASEID));
                    monitorImport.endOperationMarker(tokenUpdateProcessInstanceIndex);
                }

                // update the history
                if (importV7.historyInIndex) {
                    index = getIndexFromNumber(importV7.indexHistory);
                    if (index != null) {
                        String history = processInstanceContext.historyCase.toString();
                        if (history.length() > 255)
                            history = history.substring(0, 250) + "...";

                        processAPI.updateProcessInstanceIndex(processInstanceContext.rootProcessInstance.getId(), index, history);
                    }
                }

            } else if (operationUpdateIndexBySQL) {

                List<Object> listSqlParameters = new ArrayList<Object>();
                listSqlParameters.add(processInstanceContext.caseIdV5);
                listSqlParameters.add(processInstanceContext.rootProcessInstance.getId());

                OperationMarkerToken tokenUpdateSQLProcessInstanceIndex = monitorImport.startOperationMarker("updateSQLProcessInstanceIndex");
                String colDatabase = getColumnFilterFromNumber(importV7.indexVerification);
                if (colDatabase != null)
                    playSql.updateSql("update ARCH_PROCESS_INSTANCE set " + colDatabase + " = ? where SOURCEOBJECTID=?", listSqlParameters);
                monitorImport.endOperationMarker(tokenUpdateSQLProcessInstanceIndex);
            }

            // update the RendezVous gateway
            String rendezVous = (String) record.get(FileCsv.RENDEZVOUS);
            String[] listRendezVous = rendezVous.trim().length() == 0 ? new String[0] : rendezVous.split(FileCsv.TASKSEPARATOR);

            for (String oneRendezVous : listRendezVous) {
                // de-aply the encoding
                GatewayDefinition gateway = mapGatewaysDefinition.get(oneRendezVous);
                if (gateway == null) {
                    processStatus.error("Can't find the gateway [" + oneRendezVous + "] in process", processInstanceContext.rootProcessInstance);
                }
                // here, we have to execute the gateways

            }

            if (operationCreation == OperationCreation.ONEHUMANTASKTHENARCHIVE) {
                processAPI.setProcessInstanceState(processInstanceContext.rootProcessInstance, ProcessInstanceState.COMPLETED.toString());
            }

            importListener.caseStatus(StatusImport.OK, processInstanceContext);

            return StatusImport.OK;
        }

        catch (Exception e) {

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            if (reportExceptionError)
                processInstanceContext.error(e.getMessage() + " at: " + exceptionDetails);
            // ok, the error is too big here, we have to delete the case
            if (processInstanceContext.rootProcessInstance != null) {
                try {
                    if (processInstanceContext.isActiveCase)
                        processAPI.deleteProcessInstance(processInstanceContext.rootProcessInstance.getId());
                    else
                        processAPI.deleteArchivedProcessInstancesInAllStates(processInstanceContext.rootProcessInstance.getId());
                } catch (Exception de) {
                    processInstanceContext.error("Can't delete the created case[" + processInstanceContext.rootProcessInstance.getId() + "]");
                }
            }
            return StatusImport.ERRORIMPORT;
        }

    }

    private List<Index> listIndexes = Arrays.asList(Index.FIRST, Index.SECOND, Index.THIRD, Index.FOURTH, Index.FIFTH);

    private Index getIndexFromNumber(int range) {
        if (range >= 1 && range <= 5)
            return listIndexes.get(range - 1);
        return null;
    }

    private List<String> listFilter = Arrays.asList(ProcessInstanceSearchDescriptor.STRING_INDEX_1,
            ProcessInstanceSearchDescriptor.STRING_INDEX_2,
            ProcessInstanceSearchDescriptor.STRING_INDEX_3,
            ProcessInstanceSearchDescriptor.STRING_INDEX_4,
            ProcessInstanceSearchDescriptor.STRING_INDEX_5);

    private String getIndexFilterFromNumber(int range) {
        if (range >= 1 && range <= 5)
            return listFilter.get(range - 1);
        return null;
    }

    private List<String> listColumnFilter = Arrays.asList("STRINGINDEX1", "STRINGINDEX2", "STRINGINDEX3", "STRINGINDEX4", "STRINGINDEX5");

    private String getColumnFilterFromNumber(int range) {
        if (range >= 1 && range <= 5)
            return listColumnFilter.get(range - 1);
        return null;
    }

    /* ****************************************************************************** */
    /*                                                                                */
    /* Task Description */
    /*                                                                                */
    /* ****************************************************************************** */

    /**
     * Describe a task like in V5 by its name / processversion / processInstanceId / taskName
     */
    public static class TaskDescription {

        String processName;
        String processVersion;
        Long processInstanceId;
        String taskName;
        String contextInformation;
        String error = null;
        Date dateExecution;
        String executedBy;

        public String getKeyProcessDefinition() {
            return processName + "--" + processVersion;
        }

        /**
         * return what is the String of the object
         */
        public String toString() {
            return processName + "--" + processVersion + "--" + processInstanceId + (taskName != null ? "--" + taskName : "");
        }
    }

    /**
     * sourceList is not empty, so we expect as minumum one iteml
     * 
     * @param sourceList
     * @return
     */
    public List<TaskDescription> decodeListTaskIdentifier(String sourceList) {
        List<TaskDescription> listTaskIdentifier = new ArrayList<TaskDescription>();
        String[] listTasks = sourceList.trim().split(FileCsv.TASKSEPARATOR);

        for (String task : listTasks) {
            // de-aply the encoding: we may have an empty list here ! 
            if (task.length() > 0) {
                // coding is   Parent--1.0--1--Etape grandChild--READY
                listTaskIdentifier.add(decodeTaskIdentifier(task, CONTEXT_DECODAGE.TASK));
            }
        }
        return listTaskIdentifier;
    }

    public enum CONTEXT_DECODAGE {
        TASK, PROCESSVARIABLE, TASKVARIABLE
    }

    public TaskDescription decodeTaskIdentifier(String sourceIdentifier, CONTEXT_DECODAGE contextDecodage) {

        String[] listCode = sourceIdentifier.split("--");
        TaskDescription taskIndentifier = new TaskDescription();
        try {

            if (contextDecodage == CONTEXT_DECODAGE.TASK && listCode.length < 5) {
                taskIndentifier.error += "5 items minimum are expected <processName>--<processVersion>--<processInstanceId>--<taskName>--<context> [ --<Date:format yyyyMMdd HH:mm:ss>--<ExecutedBy>]";
                return taskIndentifier;
            }
            if (contextDecodage == CONTEXT_DECODAGE.PROCESSVARIABLE && listCode.length < 3) {
                taskIndentifier.error += "3 items minimum are expected <processName>--<processVersion>--<processInstanceId>";
                return taskIndentifier;
            }
            if (contextDecodage == CONTEXT_DECODAGE.TASKVARIABLE && listCode.length < 4) {
                taskIndentifier.error += "4 items minimum are expected <processName>--<processVersion>--<processInstanceId>--<taskName>";
                return taskIndentifier;
            }

            taskIndentifier.processName = listCode[0];
            taskIndentifier.processVersion = listCode[1];
            taskIndentifier.processInstanceId = Long.valueOf(listCode[2]);
            if (listCode.length > 3)
                taskIndentifier.taskName = listCode[3];
            if (listCode.length > 4)
                taskIndentifier.contextInformation = listCode[4];
            if (listCode.length > 5)
                taskIndentifier.dateExecution = sdf.parse(listCode[5]);
            if (listCode.length > 6)
                taskIndentifier.executedBy = listCode[6];

        } catch (Exception e) {
            taskIndentifier.error += "5 items minimum are expected <processName>--<processVersion>--<processInstanceId>--<tasksName>--<context> [ --<Date:format yyyyMMdd HH:mm:ss>--<ExecutedBy>]  :" + e.toString();
        } // don't respect the code :-(

        return taskIndentifier;
    }

    /* ****************************************************************************** */
    /*                                                                                */
    /* Update variable operation */
    /*                                                                                */
    /* ****************************************************************************** */

    private void updateVariableProcess(ProcessInstanceContext processInstanceContext) {
        for (TaskDescription taskIdentifier : processInstanceContext.variablesProcesses.content.keySet()) {

            // retrieve the processInstanceId
            Long processInstanceId = processInstanceContext.processInstanceDescription.getProcessInstanceFromProcessDefinition(taskIdentifier);

            if (processInstanceId == null)
                processInstanceContext.addErrorMessage("Can't retrieve SubProcessId[" + taskIdentifier.toString());
            else {
                Map<String, Object> variablesProcessInstance = (Map<String, Object>) processInstanceContext.variablesProcesses.content.get(taskIdentifier);

                for (String key : variablesProcessInstance.keySet()) {
                    Serializable value = null;
                    try {
                        if (variablesProcessInstance.get(key) != null) {
                            OperationMarkerToken tokenUpdateProcessDataInstance = monitorImport.startOperationMarker("updateProcessDataInstance");
                            value = transformData(taskIdentifier, processInstanceId, null, null, key, variablesProcessInstance.get(key), processAPI);

                            processAPI.updateProcessDataInstance(getV7VariableDataName(key), processInstanceId, value);
                            monitorImport.endOperationMarker(tokenUpdateProcessDataInstance);
                        }
                    } catch (Exception e) {
                        processInstanceContext.addErrorMessage(" data[" + key + "] value[" + value + "] error (nullpointer=variable does not exist) " + e.getMessage());
                    }
                }
            }

        }
    }

    private Map<Long, Boolean> cacheActorFilterOnTask = new HashMap<Long, Boolean>();

    /**
     * @param caseContext
     * @param errorIfTaskIsNotReady
     */
    private boolean updateVariableTasks(ProcessInstanceContext caseContext, boolean errorIfTaskIsNotReady) {
        // update the LocalActivity
        for (TaskDescription taskIdentifier : caseContext.variablesActivity.content.keySet()) {

            TasksInstanceIndentified tasksInstanceIndentified = caseContext.processInstanceDescription.getTaskInstanceFromProcessDefinition(taskIdentifier, processAPI, monitorImport);
            if (tasksInstanceIndentified.taskInstanceId == null) {
                if (errorIfTaskIsNotReady)
                    caseContext.addErrorMessage(
                            "Can't find task[" + taskIdentifier.taskName + "] with status {ready,executing}" + " processInstance[" + caseContext.processInstanceDescription.getProcessInstanceFromProcessDefinition(taskIdentifier) + "] - " + (taskIdentifier.error != null ? taskIdentifier.error : ""));
                return false;
            }
            Map<String, Object> variablesActivityInstance = (Map<String, Object>) caseContext.variablesActivity.content.get(taskIdentifier);
            // if the task is Failed, then we can't update the variable, so first move the state to READY, then we go back to FAILED
            if (tasksInstanceIndentified.isTaskFailed) {
                try {
                    processAPI.setActivityStateByName(tasksInstanceIndentified.taskInstanceId, ActivityStates.READY_STATE);
                } catch (Exception e) {
                    caseContext.addErrorMessage("Can't unset Failed task state, so no way to update any task data " + e.getMessage());
                    return false;

                }
            }

            for (String key : variablesActivityInstance.keySet()) {
                try {
                    if (variablesActivityInstance.get(key) != null) {
                        OperationMarkerToken tokenUpdateActivityDataInstance = monitorImport.startOperationMarker("updateActivityDataInstance");
                        Serializable value = transformData(taskIdentifier, caseContext.processInstanceDescription.getProcessInstanceFromProcessDefinition(taskIdentifier), taskIdentifier, tasksInstanceIndentified.taskInstanceId, key, variablesActivityInstance.get(key), processAPI);

                        processAPI.updateActivityDataInstance(getV7VariableDataName(key), tasksInstanceIndentified.taskInstanceId, value);
                        monitorImport.endOperationMarker(tokenUpdateActivityDataInstance);
                    }
                } catch (Exception e) {
                    caseContext.addErrorMessage("Data[" + key + "] error (nullpointer=variable does not exist) " + e.getMessage());
                }
            }

            boolean replayActorTask = false;
            // is an actor filter behing this activity ? if yes, it's time to replay it
            if (tasksInstanceIndentified.humanTask != null) {
                // a actor filter on that task ? If yes, we have to replay the actor filter at the end
                if (!cacheActorFilterOnTask.containsKey(tasksInstanceIndentified.humanTask.getFlownodeDefinitionId())) {
                    // search it
                    OperationMarkerToken tokenSearchActor = monitorImport.startOperationMarker("updateActors");
                    try {
                        DesignProcessDefinition designProcess = processAPI.getDesignProcessDefinition(tasksInstanceIndentified.humanTask.getProcessDefinitionId());
                        FlowElementContainerDefinition flowElementContainer = designProcess.getFlowElementContainer();
                        FlowNodeDefinition flowNodeDefinition = flowElementContainer.getFlowNode(tasksInstanceIndentified.humanTask.getFlownodeDefinitionId());
                        HumanTaskDefinition humanTaskDefinition = (HumanTaskDefinition) flowNodeDefinition;
                        UserFilterDefinition userFilterDefinition = humanTaskDefinition.getUserFilter();
                        cacheActorFilterOnTask.put(tasksInstanceIndentified.humanTask.getFlownodeDefinitionId(), userFilterDefinition != null);
                    } catch (Exception e) {
                        caseContext.addErrorMessage("HumanDefinition[" + tasksInstanceIndentified.humanTask.getFlownodeDefinitionId() + "] can't be retry " + e.getMessage());

                        cacheActorFilterOnTask.put(tasksInstanceIndentified.humanTask.getFlownodeDefinitionId(), false);
                    }
                    monitorImport.endOperationMarker(tokenSearchActor);
                }
                replayActorTask = cacheActorFilterOnTask.get(tasksInstanceIndentified.humanTask.getFlownodeDefinitionId());
            }

            if (tasksInstanceIndentified.isTaskFailed) {
                try {
                    processAPI.setActivityStateByName(tasksInstanceIndentified.taskInstanceId, ActivityStates.FAILED_STATE);
                    replayActorTask = true;
                } catch (Exception e) {
                    caseContext.addErrorMessage("Activity can't be retry " + e.getMessage());
                }
            }

            /**
             * replay the actor mapping
             */
            if (replayActorTask) {
                try {
                    OperationMarkerToken tokenUpdateActors = monitorImport.startOperationMarker("updateActors");
                    processAPI.updateActorsOfUserTask(tasksInstanceIndentified.taskInstanceId);
                    monitorImport.endOperationMarker(tokenUpdateActors);
                } catch (Exception e) {
                    caseContext.addErrorMessage("Activity can't be retryTask " + e.getMessage());
                }
            }
        } // end ActivityInstance
        return true;
    }

    /**
     * in V5, a data can start by a Uppder case, not in V7
     * 
     * @param key
     * @return
     */
    private String getV7VariableDataName(String key) {
        String firstCharKey = key.substring(0, 1);
        if (firstCharKey.toUpperCase().equals(firstCharKey)) {
            // key must start by a lower case
            key = firstCharKey.toLowerCase() + key.substring(1);
        }
        return key;
    }

    /* ****************************************************************************** */
    /*                                                                                */
    /* Variable Identifier */
    /*                                                                                */
    /* ****************************************************************************** */
    /**
     * the variables contains variables per taskDescription
     */
    public class VariablesDescription {

        public Map<TaskDescription, Map<String, Object>> content = new HashMap<TaskDescription, Map<String, Object>>();

        public Map<String, Object> getRootProcess(String processName, String processVersion) {
            for (TaskDescription taskIdentifier : content.keySet()) {
                if (taskIdentifier.processName.equals(processName) && taskIdentifier.processVersion.equals(processVersion))
                    return content.get(taskIdentifier);
            }
            return null;
        }
    }

    /**
     * structure:
     * VariablesActivity: {"Parent--1.0--1--Etape grandChild":{"grandChildLocalData":"It's so
     * cool"},"Parent--1.0--1--Etape1":{"localChildCall":"Let's call my grand
     * son"},"Parent--1.0--1--callChild":{"localParentCall":"Call my child "}}
     * Variables:
     * {"Parent--1.0--1":{"parentLastName":"Lalane","parentFirstName":"Patrice"},"Grandchild--1.0--1":{"grandchild_city":"Grenoble","grandchild_address":"1
     * rue revol"},"Child--1.0--1":{"child_computer":"Commodore 64","child_phone":"0476217537"}};
     */
    @SuppressWarnings("unchecked")
    public VariablesDescription decodeVariables(String variablesJson, CONTEXT_DECODAGE contextDecodage) throws Exception {
        VariablesDescription variableArtefact = new VariablesDescription();

        if (variablesJson != null) {
            Map<String, Object> variables = (Map<String, Object>) JSONValue.parse(variablesJson);
            if (variables == null)
                throw new Exception("Bad JSON decode [" + variablesJson + "]");
            for (String key : variables.keySet()) {
                TaskDescription task = decodeTaskIdentifier(key, contextDecodage);
                if (!(variables.get(key) instanceof Map))
                    throw new Exception("Bad JSON MAP expected under key [" + key + "]");

                variableArtefact.content.put(task, translateMap((Map<String, Object>) variables.get(key), false));
            }
        }
        return variableArtefact;
    }

    /* ****************************************************************************** */
    /*                                                                                */
    /* Toolbox */
    /*                                                                                */
    /* ****************************************************************************** */

    /**
     * thank to JSON parse : it don't know how to produce a correct JSON with a date
     * 
     * @param source
     * @param dateToString
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> translateMap(Map<String, Object> source, boolean dataToJson) {
        Map<String, Object> destination = new HashMap<String, Object>();
        for (String key : source.keySet()) {
            Object value = source.get(key);
            if (value == null)
                destination.put(key, value);
            else if (value instanceof Map) {
                destination.put(key, translateMap((Map<String, Object>) value, dataToJson));
            } else if (dataToJson) {
                if (value instanceof Date)
                    destination.put(key, "D:" + sdf.format((Date) value));

                else if (value instanceof Long)
                    destination.put(key, "L:" + value);

                else if (value instanceof Integer)
                    destination.put(key, "I:" + value);

                else if (value instanceof String)
                    destination.put(key, "S:" + value);

                else
                    destination.put(key, value);
            } else {
                try {
                    if (value instanceof String) {

                        String dataValue = value.toString().substring(2);
                        String code = value.toString().substring(0, 2);
                        if ("D:".equals(code))
                            destination.put(key, sdf.parse(dataValue));
                        else if ("L:".equals(code))
                            destination.put(key, Long.valueOf(dataValue));
                        else if ("I:".equals(code))
                            destination.put(key, Integer.valueOf(dataValue));
                        else
                            destination.put(key, dataValue);
                    } else
                        destination.put(key, value);

                } catch (Exception e) {
                }
            }
        }
        return destination;
    }

    /**
     * V5 return time to time a STRING as a LONG, or a INTEGER as a LONG. So, we have to deal this
     * situation at import
     */
    private Map<String, Object> cacheMapDataInstance = new HashMap<String, Object>();

    public Serializable transformData(TaskDescription taskIdentifier, Long processInstanceId, TaskDescription taskDescription, Long taskInstanceId, String key, Object value, ProcessAPI processAPI) {
        if (value == null)
            return (Serializable) value;

        DataInstance dataInstance = null;
        try {
            if (taskDescription == null) {
                // process Variable
                if (!cacheMapDataInstance.containsKey("Process"))
                    cacheMapDataInstance.put("Process", new HashMap<String, DataInstance>());

                @SuppressWarnings("unchecked")
                Map<String, DataInstance> cacheProcess = (Map<String, DataInstance>) cacheMapDataInstance.get("Process");
                if (!cacheProcess.containsKey(key)) {
                    // load it
                    dataInstance = processAPI.getProcessDataInstance(key, processInstanceId);
                    cacheProcess.put(key, dataInstance);
                }
                dataInstance = cacheProcess.get(key);
            } else {
                // task variable
                if (!cacheMapDataInstance.containsKey("Task-" + taskDescription.taskName))
                    cacheMapDataInstance.put("Task-" + taskDescription.taskName, new HashMap<String, DataInstance>());

                @SuppressWarnings("unchecked")
                Map<String, DataInstance> cacheTask = (Map<String, DataInstance>) cacheMapDataInstance.get("Task-" + taskDescription.taskName);
                if (!cacheTask.containsKey(key)) {
                    // load it
                    dataInstance = processAPI.getActivityDataInstance(key, taskInstanceId);
                    cacheTask.put(key, dataInstance);
                }
                dataInstance = cacheTask.get(key);
            }
            if (dataInstance == null)
                return (Serializable) value;
            if (value.getClass().getName().equals(dataInstance.getClassName()))
                return (Serializable) value;
            // ok, let's transform
            String valueSt = value.toString();
            if (dataInstance.getClassName().equals(String.class.getName()))
                return valueSt;
            if (dataInstance.getClassName().equals(Integer.class.getName()))
                return Integer.valueOf(valueSt);
            if (dataInstance.getClassName().equals(Long.class.getName()))
                return Long.valueOf(valueSt);
        } catch (Exception e) {
            // we didn't find the variable, so we can't transform it
        }
        // we don't manage the another situation, so return the object as it; good luck
        return (Serializable) value;
    }

    /* ****************************************************************************** */
    /*                                                                                */
    /* ProcessInstanceArtefact */
    /*                                                                                */
    /* ****************************************************************************** */

    public static boolean isIdentical(String key1, String key2) {
        key1 = key1.replaceAll("ç", "c").replaceAll("ã", "a");
        key2 = key2.replaceAll("ç", "c").replaceAll("ã", "a");
        return key1.equalsIgnoreCase(key2);

    }

    /**
     * give a change to find the process definition in V5
     * 
     * @param processName
     * @param processVersion
     * @param processAPI
     * @return
     * @throws Exception
     */
    private Long getPermissiveProcessDefinition(String processName, String processVersion, ProcessAPI processAPI) throws Exception {
        try {
            return processAPI.getProcessDefinitionId(processName, processVersion);
        } catch (Exception e) {
        }
        // give a second change, replace all _ by a space
        processName = processName.replaceAll("_", " ");
        return processAPI.getProcessDefinitionId(processName, processVersion);
    }
}
