package org.bonitasoft.migrate.repair;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.migrate.tool.Toolbox;

class ProcessStatus {
    static Logger logger = Logger.getLogger(ProcessStatus.class.getName());

  String fileName;
  String processName;
  String processVersion;
  ProcessDefinition processDefinition;
  int nbCasesDetected = 0;
  int nbCaseImported = 0;
  int nbCaseAlreadyPresent = 0;
  int nbCaseError = 0;
  long timeExecution;
  String endEventName;
  String oneHumanTask;

  private Long currentProcessInstanceId = null;
  public List<String> listErrors = new ArrayList<String>();

  public String toString() {
    String status = "";
    if (nbCaseError > 0)
      status = "** ERROR **";
    else if (nbCasesDetected == nbCaseAlreadyPresent + nbCaseImported)
      status = "** COMPLETE **";
    else
      status = "--";
    return getProcessDescription() + " Cases Detected:" + nbCasesDetected + " Imported:" + nbCaseImported + " AlreadyPresent:" + nbCaseAlreadyPresent + " error:" + nbCaseError + " " + status + " in " + Toolbox.getHumanTime(timeExecution);
  }

  public String getProcessDescription() {
    if (processDefinition == null)
      return "Process[null]";
    return "Process[" + processDefinition.getName() + "--" + processDefinition.getVersion() + "]";
  }

  public String error(String message, ProcessInstance processInstance) {
    String error = "error detected:" + message + " " + getProcessDescription();
    if (processInstance != null) {
      error += "Case[" + processInstance.getId() + "]";
      /*
       * we count CASE error, not ERRORS, so if this is the current processid, do not do a ++
       */
      if (currentProcessInstanceId == null || currentProcessInstanceId != processInstance.getId())
        nbCaseError++;
      currentProcessInstanceId = processInstance.getId();
    }
    listErrors.add(error);
    logger.severe(error);
    return error;
  }

 

  public int getNbCasesProcessed() {
    return nbCaseImported + nbCaseAlreadyPresent + nbCaseError;
  }
}
