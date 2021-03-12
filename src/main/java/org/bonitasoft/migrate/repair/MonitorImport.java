package org.bonitasoft.migrate.repair;

import org.bonitasoft.migrate.tool.OperationTimeTracker;
import org.bonitasoft.migrate.tool.Toolbox;

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
      expl += " Estimated duration: " + Toolbox.getHumanTime(durationTotal) + ", Estimated time left:" + Toolbox.getHumanTime(durationTotal - durationAdv);
    // ImportV7.logger.info("    "+expl);

    return expl;
  }

  /**
   * Keep an operation TimeTracker
   */
  private OperationTimeTracker operationTimeTracker = new OperationTimeTracker();
  
  public OperationTimeTracker getOperationTimeTracker() {
      return operationTimeTracker;
  }
}
