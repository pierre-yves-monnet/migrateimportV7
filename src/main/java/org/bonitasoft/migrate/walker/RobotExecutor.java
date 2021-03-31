package org.bonitasoft.migrate.walker;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.bpm.contract.ContractDefinition;
import org.bonitasoft.engine.bpm.flownode.ActivityStates;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.UserTaskDefinition;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.DesignProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.migrate.tool.OperationTimeTracker.OperationMarkerToken;
import org.bonitasoft.migrate.walker.RobotDataItem.RobotStep;

import com.bonitasoft.engine.api.ProcessAPI;
import com.bonitasoft.engine.bpm.process.Index;
import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceSearchDescriptor;

public class RobotExecutor {

    Logger logger = Logger.getLogger(RobotExecutor.class.getName());

    APISession apiSession;
    ProcessAPI processAPI;
    IdentityAPI identityAPI;
    ImportWalker importWalker;

    RobotExecutor(ImportWalker importWalker, APISession apiSession, ProcessAPI processAPI, IdentityAPI identityAPI) {
        this.importWalker = importWalker;
        this.apiSession = apiSession;
        this.processAPI = processAPI;
        this.identityAPI = identityAPI;
    }

    public class ExecuteResult {

        public StringBuilder log = new StringBuilder();
        public boolean alreadyImported = false;
        public boolean allIsCorrect = false;
        public ProcessInstance processInstance = null;
        public StringBuilder errorMessage = new StringBuilder();
    }

    /**
     * Return the Log Execution
     * 
     * @param robotDataItem
     * @return
     * @throws Exception
     */
    public ExecuteResult execute(RobotDataItem robotDataItem) throws Exception {

        ExecuteResult executeResult = new ExecuteResult();
        TransformContract transformContract = new TransformContract();

        // Creation
        Long processDefinitionId = processAPI.getProcessDefinitionId(robotDataItem.processName, robotDataItem.processVersion);
        DesignProcessDefinition pdef = processAPI.getDesignProcessDefinition(processDefinitionId);

        // if the case already exist with the refrence ? 
        if (robotDataItem.reference != null) {
            Long processId = caseIdWithReference(robotDataItem.reference, processDefinitionId);
            if (processId != null) {
                executeResult.log.append("Already exist reference[" + robotDataItem.reference + "] in CaseId[" + processId + "]");
                return executeResult;
            }
        }

        // Case creation 
        ContractDefinition processContract = processAPI.getProcessContract(processDefinitionId);
        User userCreation = (robotDataItem.userName == null) ? null : identityAPI.getUserByUserName(robotDataItem.userName);
        Map<String,Serializable> contractTransformed = transformContract.transformContract(robotDataItem.contract, processContract);
        
        
        if (userCreation != null) {
            executeResult.processInstance = processAPI.startProcessWithInputs(userCreation.getId(), processDefinitionId, contractTransformed);
        } else {
            executeResult.processInstance = processAPI.startProcessWithInputs(processDefinitionId, contractTransformed);
        }
        
        // set the reference 
        if (robotDataItem.reference != null) {

            OperationMarkerToken tokenUpdateProcessInstanceIndex = importWalker.getOperationTimeTracker().startOperationMarker("updateProcessInstanceIndex");
            processAPI.updateProcessInstanceIndex(executeResult.processInstance.getId(), Index.FIRST,robotDataItem.reference);
            importWalker.getOperationTimeTracker().endOperationMarker(tokenUpdateProcessInstanceIndex);
        }
        
        executeResult.log.append("CaseId[" + executeResult.processInstance.getId() + "] created;");
        // Execute each task now
        for (RobotStep robotStep : robotDataItem.listSteps) {
            // search the task 
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 100);
            sob.filter(HumanTaskInstanceSearchDescriptor.PROCESS_INSTANCE_ID, executeResult.processInstance.getId());
            sob.filter(HumanTaskInstanceSearchDescriptor.NAME, robotStep.taskName);
            sob.filter(HumanTaskInstanceSearchDescriptor.STATE_NAME, ActivityStates.READY_STATE);

            HumanTaskInstance foundHumanTask = null;
            int countExecution = 0;

            while (countExecution < 5 && foundHumanTask==null) {
                countExecution++;

                SearchResult<HumanTaskInstance> searchHumanTask = processAPI.searchHumanTaskInstances(sob.done());
                if (searchHumanTask.getCount() == 0 && countExecution < 5) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    foundHumanTask = searchHumanTask.getResult().get(0);
                }
            } // end search humanTasks 
            if (foundHumanTask == null) {
                executeResult.allIsCorrect = false;
                executeResult.log.append("Waiting task[" + robotStep.taskName + "] never show up in " + (countExecution * 500) + "ms");
                return executeResult;
            }
            // execute it
            try {
                // if the getExecutedBy return null, then we use the first user we found
                User userTask = (robotStep.userName == null) ? null : identityAPI.getUserByUserName(robotStep.userName);
                Long userTaskId = (userTask == null ? -1 : userTask.getId());
                processAPI.assignUserTask(foundHumanTask.getId(), userTaskId);

                UserTaskDefinition task = (UserTaskDefinition) pdef.getFlowElementContainer().getActivity(foundHumanTask.getName());
                ContractDefinition contractDefinition = task.getContract();
                
                contractTransformed = transformContract.transformContract(robotStep.contract,contractDefinition);

                processAPI.executeUserTask(userTaskId, foundHumanTask.getId(), contractTransformed);
                executeResult.log.append("task[" + robotStep.taskName + "] executed;");
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionDetails = sw.toString();
                executeResult.log.append("Error executed task[" + robotStep.taskName + "] " + e.getMessage() + " at " + exceptionDetails);
                executeResult.allIsCorrect = false;
                return executeResult;
            }
        }

        executeResult.allIsCorrect = true;
        return executeResult;
    }

    public Long caseIdWithReference(String reference, Long processDefinitionId) {
        try {
            SearchOptionsBuilder searchOptionsBuilder = new SearchOptionsBuilder(0, 100);

            searchOptionsBuilder.filter(ProcessInstanceSearchDescriptor.STRING_INDEX_1, reference);
            searchOptionsBuilder.filter(ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDefinitionId);
            // must search in ACTIVE and ARCHIVE fiter...

            OperationMarkerToken tokenSearchProcessInstances = importWalker.getOperationTimeTracker().startOperationMarker("searchProcessInstances");
            SearchResult<ProcessInstance> searchProcessInstance = processAPI.searchProcessInstances(searchOptionsBuilder.done());
            importWalker.getOperationTimeTracker().endOperationMarker(tokenSearchProcessInstances);

            if (!searchProcessInstance.getResult().isEmpty()) {
                return searchProcessInstance.getResult().get(0).getRootProcessInstanceId();
            }
            // seach in archive now
            tokenSearchProcessInstances = importWalker.getOperationTimeTracker().startOperationMarker("searchProcessInstances");
            SearchResult<ArchivedProcessInstance> searchArchiveProcessInstance = processAPI.searchArchivedProcessInstances(searchOptionsBuilder.done());
            importWalker.getOperationTimeTracker().endOperationMarker(tokenSearchProcessInstances);

            if (!searchArchiveProcessInstance.getResult().isEmpty())
                return searchArchiveProcessInstance.getResult().get(0).getRootProcessInstanceId();
            return null;
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            logger.severe("Execption during search reference [" + reference + "] in processDefinitionId[" + processDefinitionId + "] : " + e.getMessage() + " at " + exceptionDetails);
            return null;
        }
    }
}
