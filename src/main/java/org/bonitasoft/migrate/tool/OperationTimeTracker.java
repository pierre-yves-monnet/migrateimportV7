package org.bonitasoft.migrate.tool;

import java.util.HashMap;
import java.util.Map;

/* ******************************************************************************** */
/*
 * Operation Marker
 */
/* ******************************************************************************** */
public class OperationTimeTracker {

    /**
     * OperationMarker
     * use a monitorImport.startOperationMarker() to make the begining of the operation, the a
     * monitorImport.collectOperationMarker( operationToken );
     * Do not include 2 operation marker
     */

    public class OperationMarker {

        public String name;
        public int nbOccurence = 0;
        public long totalTime = 0;

        public String toString() {
            return name + " " + (nbOccurence == 0 ? "nothing" : totalTime / nbOccurence + " ms") + " on " + nbOccurence;
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

    public class OperationMarkerToken {

        public long timeMarker;
        public String operationName;

        public OperationMarkerToken(String operationName) {
            {
                this.operationName = operationName;
                timeMarker = System.currentTimeMillis();
            }
        }
    }
}
