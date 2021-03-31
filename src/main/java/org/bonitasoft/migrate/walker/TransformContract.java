package org.bonitasoft.migrate.walker;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.bonitasoft.engine.bpm.contract.ContractDefinition;
import org.bonitasoft.engine.bpm.contract.InputDefinition;
import org.bonitasoft.engine.bpm.contract.Type;



public class TransformContract {

    private static final String CST_OFFSETDATETIME = "yyyy-MM-dd'T'kk:mm:ss.SSS'Z'";
    private static final String CST_LOCALDATETIME = "yyyy-MM-dd'T'kk:mm:ss.SSS";
    private static final String CST_DATEFORMAT = "yyyy-MM-dd";

    // ContractDefinition processContract = processAPI.getProcessContract(processInstance.processDefinitionId);
    Logger logger = Logger.getLogger(TransformContract.class.getName());

    public Map<String, Serializable> transformContract(Map<String, Serializable> data, ContractDefinition contractDefinition) {
        HashMap<String, Serializable> transformedData = new HashMap<>();
        for (InputDefinition inputDefinition : contractDefinition.getInputs()) {
            Serializable dataInput = data.get(inputDefinition.getName());
            transformedData.put(inputDefinition.getName(), transform(inputDefinition, dataInput));
        }
        logger.info( traceContract( "", transformedData, ""));
        return transformedData;
    }

    private Serializable transform(InputDefinition inputDefinition, Serializable dataInput) {
        if (dataInput == null) {
            return null;
        }
        DateTimeFormatter formatterLocalDateTime = DateTimeFormatter.ofPattern( CST_LOCALDATETIME);
        DateTimeFormatter formatterOffsetDateTime = DateTimeFormatter.ofPattern(CST_OFFSETDATETIME );
        if ("null".equals(dataInput))
            return null;
        try {
            if (inputDefinition.isMultiple()) {
                // data must be a list 
                if (!(dataInput instanceof List)) {
                    logger.severe("Data Input [" + inputDefinition.getName() + "] is not a list, detect " + dataInput.getClass().getName());
                    return null;
                }
                ArrayList<Serializable> listDataTransformed = new ArrayList<>();
                for (Serializable dataInputItem : (List<Serializable>) dataInput) {
                    // May be a Long, Date, or Map
                    Serializable dataInputItemTransformed = transform(inputDefinition, dataInputItem);
                    listDataTransformed.add(dataInputItemTransformed);
                }
                return listDataTransformed;
                
            } else if (inputDefinition.hasChildren()) {
                HashMap mapDataItem = new HashMap();
                for (InputDefinition subInput : inputDefinition.getInputs()) {
                    mapDataItem.put(subInput.getName(), transform(subInput, ((Map<String, Serializable>) dataInput).get(subInput.getName())));
                }
                return mapDataItem;
                
            } else if (inputDefinition.getType() == Type.BOOLEAN) {
                if ("1".equals(dataInput.toString()))
                    return Boolean.TRUE;
                if ("0".equals(dataInput.toString()))
                    return Boolean.FALSE;
                return Boolean.valueOf(dataInput.toString());

            } else if (inputDefinition.getType() == Type.LONG) {
                return Long.parseLong(dataInput.toString());

            } else if (inputDefinition.getType() == Type.DATE) {
                SimpleDateFormat sdf = new SimpleDateFormat(CST_DATEFORMAT);
                return sdf.parse(dataInput.toString());
                
            } else if (inputDefinition.getType() == Type.LOCALDATE) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CST_DATEFORMAT);
                return LocalDate.parse(dataInput.toString(), formatter);
                
            } else if (inputDefinition.getType() == Type.LOCALDATETIME) {
                try {
                    return LocalDateTime.parse(dataInput.toString(), formatterLocalDateTime);
                } catch(Exception e ) {
                    return LocalDateTime.parse(dataInput.toString(), formatterOffsetDateTime);
                }
                
            } else if (inputDefinition.getType() == Type.OFFSETDATETIME) {
                try {
                    return LocalDateTime.parse(dataInput.toString(), formatterLocalDateTime);
                } catch(Exception e ) {
                    return LocalDateTime.parse(dataInput.toString(), formatterOffsetDateTime);
                }

            } else if (inputDefinition.getType() == Type.DECIMAL) {
                return Double.parseDouble(dataInput.toString());

            } else if (inputDefinition.getType() == Type.INTEGER) {
                return Integer.parseInt(dataInput.toString());




            } else {
                return dataInput.toString();

            }
        } catch (Exception e) {
            logger.severe("Data Input [" + inputDefinition.getName() + "] Type[" + inputDefinition.getType().toString() + "] Data[" + dataInput + "] Exception " + e.toString());
            return null;

        }
    }
    
    private String traceContract( String name, Serializable contractTransformed, String indentation) {
        StringBuilder st = new StringBuilder();
        if (contractTransformed instanceof Map) {
            st.append(indentation+" - "+name+" (MAP) { \n");
            for (Entry<String,Serializable> entryMap : ((Map<String,Serializable>) contractTransformed).entrySet()) {
                st.append(traceContract(entryMap.getKey(), entryMap.getValue(), indentation+"  "));
            }
            st.append(indentation+"] /* end "+name+" */\n");
        } else if (contractTransformed instanceof List) {
            st.append(indentation+" - "+name+" (LIST) [ \n");
            for (Serializable entryIndex : ((List<Serializable>) contractTransformed)) {
                st.append(traceContract("", entryIndex, indentation+"  "));
            }
            st.append(indentation+"] /* end "+name+" */\n");
        }  else if (contractTransformed == null) {
            st.append(indentation+" - "+name+" = null\n");
        } else { 
            st.append(indentation+" - "+name+" ("+contractTransformed.getClass().getName() + ") ="+contractTransformed.toString()+"\n");
        }
        return st.toString();
    }
}
