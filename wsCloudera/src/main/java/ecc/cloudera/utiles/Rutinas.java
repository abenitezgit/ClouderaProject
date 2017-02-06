/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ecc.cloudera.utiles;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.codehaus.jackson.map.SerializationConfig;

/**
 *
 * @author andresbenitez
 */
public class Rutinas {
    
    //Constructor de la clase
    //
    public Rutinas() {
    }
    
    public String getDateNow() {
        try {
            //Extrae Fecha de Hoy
            //
            Date today;
            SimpleDateFormat formatter;
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            today = new Date();
            return formatter.format(today);  
        } catch (Exception e) {
            return null;
        }
    }

    public String getDateNow(String xformat) {
        try {
            //Extrae Fecha de Hoy
            //
            Date today;
            SimpleDateFormat formatter;
            formatter = new SimpleDateFormat(xformat);
            today = new Date();
            return formatter.format(today);  
        } catch (Exception e) {
            return null;
        }
    }
    
    public String sendError(int errCode, String errMesg) {
        JSONObject jData = new JSONObject();
        JSONObject jHeader = new JSONObject();
    
        jData.put("errMesg", errMesg);
        jData.put("errCode", errCode);
        
        jHeader.put("data",jData);
        jHeader.put("result", "error");
            
        return jHeader.toString();
    }
    
    public String sendError(int errCode) {
        String errMesg;
        
        switch (errCode) {
            case 90: 
                    errMesg = "error de entrada";
                    break;
            case 80: 
                    errMesg = "servicio offlne";
                    break;
            case 60:
                    errMesg = "TX no autorizada";
                    break;
            case 10:
                    errMesg = "procID ya se encuentra en poolProcess...";
                    break;
            default: 
                    errMesg = "error desconocido";
                    break;
        }
        
        JSONObject jData = new JSONObject();
        JSONObject jHeader = new JSONObject();
    
        jData.put("errMesg", errMesg);
        jData.put("errCode", errCode);
        
        jHeader.put("data",jData);
        jHeader.put("result", "error");
            
        return jHeader.toString();
    }
    
    public String sendOkTX() {
        
        JSONObject jData = new JSONObject();
        JSONObject jHeader = new JSONObject();
        
        jHeader.put("data", jData);
        jHeader.put("result", "OK");
            
        return jHeader.toString();
    }
    
    public static double getProcessCpuLoad() throws Exception {
        MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
        ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

        if (list.isEmpty())     return Double.NaN;

        Attribute att = (Attribute)list.get(0);
        Double value  = (Double)att.getValue();

        // usually takes a couple of seconds before we get real values
        if (value == -1.0)      return Double.NaN;
        // returns a percentage value with 1 decimal point precision
        return ((int)(value * 1000) / 10.0);
    }
    
     public String sendDate() {
        try {
            JSONObject jo = new JSONObject();
            JSONArray ja = new JSONArray();
            JSONObject mainjo = new JSONObject();

            jo.put("fecha", getDateNow("yyyy-MM-dd HH:mm:ss"));
            ja.put(jo);
            mainjo.put("data", ja);
            mainjo.put("result", "OK");
            return mainjo.toString();
        } catch (Exception e) {
            return sendError(99, e.getMessage());
        }
    }
    
     public String serializeObjectToJSon (Object object, boolean formated) throws IOException {
        org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper();
        
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, formated);
        
        return mapper.writeValueAsString(object);
    }
    
    @SuppressWarnings("unchecked")
	public Object serializeJSonStringToObject (String parseJson, @SuppressWarnings("rawtypes") Class className) throws IOException {
        org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper();
        
        return mapper.readValue(parseJson, className);
    }    
}
