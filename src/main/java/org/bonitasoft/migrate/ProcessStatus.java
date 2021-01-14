package org.bonitasoft.migrate;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstance;

class ProcessStatus {

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
    return getProcessDescription() + " Cases Detected:" + nbCasesDetected + " Imported:" + nbCaseImported + " AlreadyPresent:" + nbCaseAlreadyPresent + " error:" + nbCaseError + " " + status + " in " + getHumanTime(timeExecution);
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
    ImportV7.logger.severe(error);
    return error;
  }

  /**
   * @param time
   * @return
   */
  public static String getHumanTime(long time) {
    long hour = time / 1000 / 60 / 60;
    time -= hour * 1000 * 60 * 60;

    long minute = time / 1000 / 60;
    time -= minute * 1000 * 60;

    long second = time / 1000;
    time -= second * 1000;
    // don't display second and ms if we are more than 1 hour
    return (hour > 0 ? hour + " h " : "") + minute + " mn " + (hour < 1 ? second + " s " + time + " ms" : "");

  }

  public int getNbCasesProcessed() {
    return nbCaseImported + nbCaseAlreadyPresent + nbCaseError;
  }
}
