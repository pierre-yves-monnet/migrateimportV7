package org.bonitasoft.migrate;

import java.util.HashMap;
import java.util.Map;

/**
 * track the time to finish the job
 */
public class MonitorImport {

  public int totalProcess = 0;
  public int totalCase = 0;

  public long timeStartImport = 0;
  public long timeEndExport = 0;

  // current advance
  public int advProcess;
  public int advCase;

  public void startImport(int totalProcess, int totalCase) {
    this.totalProcess = totalProcess;
    this.totalCase = totalCase;
    this.advProcess = 0;
    this.advCase = 0;

    this.timeStartImport = System.currentTimeMillis();
  }

  public void addStepProcess(int numberOfCaseProcessed) {
    this.advCase += numberOfCaseProcessed;
    this.advProcess++;
  }

  public void endImport() {
    timeEndExport = System.currentTimeMillis();
  }

  public long getTotalTime() {
    return timeEndExport - timeStartImport;
  }

  public String getEstimationToEnd(int caseInCurrentStep, int corrects, int exists, int errors) {
    double percentAdv = totalCase == 0 ? 100 : (double) (100 * (advCase + caseInCurrentStep) / totalCase);
    long durationAdv = System.currentTimeMillis() - timeStartImport;
    long durationTotal = percentAdv == 0 ? 0 : (long) (100.0 * durationAdv / percentAdv);
    String expl = (advCase + caseInCurrentStep) + "/" + totalCase + " (" + String.format("%.1f", percentAdv) + " %)";
    expl += " {" + corrects + " ok, " + exists + " exists, " + errors + " errors}";
    if (percentAdv > 0)
      expl += " Estimated duration: " + ProcessStatus.getHumanTime(durationTotal) + ", Estimated time left:" + ProcessStatus.getHumanTime(durationTotal - durationAdv);
    // ImportV7.logger.info("    "+expl);

    return expl;
  }

  /* ******************************************************************************** */
  /*
   * Operation Marker
   */
  /* ******************************************************************************** */

  /**
   * OperationMarker
   * use a monitorImport.startOperationMarker() to make the begining of the operation, the a
   * monitorImport.collectOperationMarker( operationToken );
   * Do not include 2 operation marker
   */
  public static class OperationMarker {

    public String name;
    public int nbOccurence = 0;
    public long totalTime = 0;

    public String toString() {
      return name + " " + (nbOccurence == 0 ? "nothing" : totalTime / nbOccurence + " ms") + " on " + nbOccurence;
    }

  }

  public static class OperationMarkerToken {

    public long timeMarker;
    public String operationName;

    public OperationMarkerToken(String operationName) {
      {
        this.operationName = operationName;
        timeMarker = System.currentTimeMillis();
      }
    }
  }

  private Map<String, OperationMarker> mapOperationMarker = new HashMap<String, OperationMarker>();

  public OperationMarkerToken startOperationMarker(String operationName) {
    return new OperationMarkerToken(operationName);
  }

  public void endOperationMarker(OperationMarkerToken operationToken) {
    long timeOperation = System.currentTimeMillis() - operationToken.timeMarker;
    OperationMarker operationMarker = mapOperationMarker.get(operationToken.operationName);
    if (operationMarker == null)
      operationMarker = new OperationMarker();
    operationMarker.name = operationToken.operationName;
    operationMarker.nbOccurence++;
    operationMarker.totalTime += timeOperation;
    mapOperationMarker.put(operationToken.operationName, operationMarker);
  }

  public String getOperationMarkerInfo() {
    String result = "";
    for (OperationMarker operation : mapOperationMarker.values())
      result += operation.toString() + ",  ";
    return result;
  }
}
