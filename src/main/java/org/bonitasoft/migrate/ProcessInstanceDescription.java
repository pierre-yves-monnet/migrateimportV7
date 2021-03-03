package org.bonitasoft.migrate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.bpm.flownode.ActivityStates;
import org.bonitasoft.engine.bpm.flownode.FlowNodeInstance;
import org.bonitasoft.engine.bpm.flownode.FlowNodeInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.FlowNodeType;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.migrate.ImportProcessMgmt.TaskDescription;

import org.bonitasoft.migrate.MonitorImport.OperationMarkerToken;

import com.bonitasoft.engine.api.ProcessAPI;

/**
 * From a RootProcessInstance, get all the different Actives tasks and all processinstance (root,
 * sub call, etc...)
 */
public class ProcessInstanceDescription {

    
    public static class TasksInstanceIndentified {

        public Long taskInstanceId = null;
        public String taskState;
        public List<FlowNodeInstance> listFlowNodeInstance = new ArrayList<FlowNodeInstance>();
        public boolean isTaskFailed = false;
        public HumanTaskInstance humanTask = null;
        public String error = null;
    }
    
    
    private Long processInstanceId = null;
    SearchResult<FlowNodeInstance> searchFlowNode;
    private Map<String, Long> mapProcessInstanceByDefinition = new HashMap<String, Long>();

    public void load(Long rootProcessInstance, ProcessAPI processAPI, MonitorImport monitorImport) throws SearchException, ProcessDefinitionNotFoundException {
        this.processInstanceId = rootProcessInstance;
        executeSearchFlowNode(processAPI, monitorImport);

        // get then the list of ProcessId by a processName/Process Version
        for (FlowNodeInstance flowNodeInstance : searchFlowNode.getResult()) {

            TaskDescription taskIdentifier = new TaskDescription();
            ProcessDefinition processDefinition = processAPI.getProcessDefinition(flowNodeInstance.getProcessDefinitionId());
            taskIdentifier.processName = processDefinition.getName();
            taskIdentifier.processVersion = processDefinition.getVersion();
            mapProcessInstanceByDefinition.put(taskIdentifier.getKeyProcessDefinition(), flowNodeInstance.getParentProcessInstanceId());
        }
    }

    public Long getProcessInstanceFromProcessDefinition(TaskDescription taskIdentifierProcess) {
        String processName = taskIdentifierProcess.getKeyProcessDefinition();
        Long subProcessId = mapProcessInstanceByDefinition.get(processName);
        if (subProcessId != null)
            return subProcessId;
        // give a chance : may be exist without the _, 
        processName = processName.replaceAll("_", " ");
        subProcessId = mapProcessInstanceByDefinition.get(processName);
        if (subProcessId != null) {
            mapProcessInstanceByDefinition.put(processName, subProcessId);
            return subProcessId;
        }
        for (String key : mapProcessInstanceByDefinition.keySet()) {
            if (ImportProcessMgmt.isIdentical(key, processName)) {
                subProcessId = mapProcessInstanceByDefinition.get(key);
                mapProcessInstanceByDefinition.put(processName, subProcessId);
                return subProcessId;
            }
        }
        return null;
    }



    /**
     * get the information on the new V7 task instance according the V5 information
     * 
     * @param taskIdentifierProcess
     * @param processAPI
     * @return
     */
    public ProcessInstanceDescription.TasksInstanceIndentified getTaskInstanceFromProcessDefinition(TaskDescription taskIdentifierProcess, ProcessAPI processAPI, MonitorImport monitorImport) {
        ProcessInstanceDescription.TasksInstanceIndentified tasksInstanceIndentified = new TasksInstanceIndentified();
        Long processInstanceId = getProcessInstanceFromProcessDefinition(taskIdentifierProcess);
        if (processInstanceId == null) {
            tasksInstanceIndentified.error = "Can't find a processInstance from " + taskIdentifierProcess.toString();
            return tasksInstanceIndentified;
        }
        // search in the result
        int count = 10;
        boolean allIsInitialised = false;
        while (count > 0 && !allIsInitialised) {
            count--;
            allIsInitialised = true;
            OperationMarkerToken tokenWaitInitializing = monitorImport.startOperationMarker("waitInitializing");

            for (FlowNodeInstance flowNodeInstance : searchFlowNode.getResult()) {
                if (flowNodeInstance.getState().equals(ActivityStates.INITIALIZING_STATE)) {
                    // this is an error for the moment, should purge it
                    tasksInstanceIndentified.error = "Found a initializing task flowNodeId[" + flowNodeInstance.getId() + "]";

                    // let's finish the initialisation, it's important to be sure that all local variable will be here
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    allIsInitialised = false;
                }
            }
            if (!allIsInitialised)
                try {
                    executeSearchFlowNode(processAPI, monitorImport);
                } catch (Exception e) {
                }
            monitorImport.endOperationMarker(tokenWaitInitializing);

        }

        tasksInstanceIndentified.listFlowNodeInstance = new ArrayList<FlowNodeInstance>();

        for (FlowNodeInstance flowNodeInstance : searchFlowNode.getResult()) {
            if (flowNodeInstance.getParentProcessInstanceId() != processInstanceId)
                continue;
            tasksInstanceIndentified.listFlowNodeInstance.add(flowNodeInstance);

            /*
             * List<DataInstance> listActivityDataInstance=
             * processAPI.getActivityDataInstances(flowNodeInstance.getId(),0,1000);
             * String listSt="";
             * for (DataInstance dataInstance : listActivityDataInstance)
             * {
             * listSt+=dataInstance.getName()+"("+dataInstance.getContainerType()+"), ";
             * }
             */

            // state is flowNodeInstance.getState().equals( "failed") ? Return null because no variable can be updated
            if (flowNodeInstance.getName().equals(taskIdentifierProcess.taskName)
                    && (flowNodeInstance.getState().equals(ActivityStates.READY_STATE)
                            || flowNodeInstance.getState().equals(ActivityStates.EXECUTING_STATE)
                            || flowNodeInstance.getState().equals(ActivityStates.FAILED_STATE)
                            || flowNodeInstance.getState().equals(ActivityStates.INITIALIZING_STATE))
                    && (flowNodeInstance.getType() != FlowNodeType.LOOP_ACTIVITY)) {
                tasksInstanceIndentified.taskInstanceId = flowNodeInstance.getId();
                tasksInstanceIndentified.taskState = flowNodeInstance.getState();
                // so task has to be retry after the task variable update
                tasksInstanceIndentified.isTaskFailed = flowNodeInstance.getState().equals(ActivityStates.FAILED_STATE) || flowNodeInstance.getState().equals(ActivityStates.INITIALIZING_STATE);
                if (flowNodeInstance instanceof HumanTaskInstance)
                    tasksInstanceIndentified.humanTask = (HumanTaskInstance) flowNodeInstance;
                // no more error now, we get it
                tasksInstanceIndentified.error = null;

            }
        }
        return tasksInstanceIndentified;
    }

    /**
     * search all flownodes from the taskId
     * 
     * @throws SearchException
     */
    private void executeSearchFlowNode(ProcessAPI processAPI, MonitorImport monitorImport) throws SearchException {

        OperationMarkerToken tokenSearchFlowNode = monitorImport.startOperationMarker("searchFlowNode");

        SearchOptionsBuilder searchOptionsBuilder = new SearchOptionsBuilder(0, 1000);
        searchOptionsBuilder.filter(FlowNodeInstanceSearchDescriptor.ROOT_PROCESS_INSTANCE_ID,
                processInstanceId);

        // SearchResult<ActivityInstance> searchActivity =
        // processAPI.searchActivities(searchOptionsBuilder.done());
        searchFlowNode = processAPI.searchFlowNodeInstances(searchOptionsBuilder.done());

        monitorImport.endOperationMarker(tokenSearchFlowNode);

    }
}