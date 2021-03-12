package org.bonitasoft.migrate.walker;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;

public class RobotDataItem {

    public String processName;
    public String processVersion;
    public String userName;
    public String reference;
    public Map<String,Serializable> contract;
    public List<RobotStep> listSteps = new ArrayList<>();
    
    public class RobotStep {
        public String taskName;
        public String userName;
        public Map<String,Serializable> contract;
    }

    /**
     * Read the data from a File
     * @param fileName
     * @throws Exception
     */
    public void readFromFile( String fileName ) throws Exception {
        try (FileInputStream fis = new FileInputStream(fileName); BufferedReader br = new BufferedReader(new InputStreamReader(fis,  StandardCharsets.UTF_8))) {
        
        StringBuilder data = new StringBuilder();
        String str;
        while ((str = br.readLine()) != null) {
            data.append( str);
        }
         readFromString( data.toString() );
        } catch(Exception e) {
            throw e;
        }
    }
    
    /**
     * Read the data from the JSON string
     * @param dataSt
     * @throws Exception
     */
    public void readFromString( String dataSt ) throws Exception {
        final HashMap<String, Object> mainJson = (HashMap<String, Object>) JSONValue.parse(dataSt);
        
        processName = (String) mainJson.get("processName");
        processVersion = (String) mainJson.get("processVersion");
        userName = (String)mainJson.get("userName");
        reference = (String)mainJson.get("reference");
        userName = (String)mainJson.get("userName");
        if (userName!=null && userName.trim().length()==0)
            userName=null;
        contract = (Map<String,Serializable>) mainJson.get("instanciationContract");
        List<Map<String,Object>> steps = (List)  mainJson.get("steps");
        
        for (Map<String,Object> stepJson : steps)
        {
            RobotStep robotStep = new RobotStep();
            robotStep.taskName= (String) stepJson.get("taskName");
            robotStep.userName= (String) stepJson.get("userName");
            if (robotStep.userName!=null && robotStep.userName.trim().length()==0)
                robotStep.userName=null;
            
            robotStep.contract = (Map<String,Serializable>) stepJson.get("contract");
            listSteps.add( robotStep );
        }
    }
    
    
}
