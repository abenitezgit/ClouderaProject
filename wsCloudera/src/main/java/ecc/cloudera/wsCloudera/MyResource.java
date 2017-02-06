package ecc.cloudera.wsCloudera;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import ecc.cloudera.dataAccess.ClouderaHB;
import ecc.cloudera.model.DataRequest;
import ecc.cloudera.model.Grabacion;
import ecc.cloudera.utiles.Rutinas;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("getGrab")
public class MyResource {

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getIt() {
    	//Variable de Respuesta
    	String respuesta;
    	
    	//Parametros de Entrada
    	DataRequest request = new DataRequest();
    	request.setSkill("SKILL_103EPH");
    	request.setFechaDesde("2017011212");
    	request.setFechaHasta("2017011213");
    	request.setTableName("grabaciones");
    	request.setLimit(10);
    	
    	ClouderaHB connCloud = new ClouderaHB();
    	
    	List<String> lstFileConfs = new ArrayList<>();
//    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-cloudera/conf/hbase-site.xml");
//    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-cloudera/conf/core-site.xml");
//    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-cloudera/conf/hdfs-site.xml");
//    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-quickcloud/conf/hbase-site.xml");
//    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-quickcloud/conf/core-site.xml");
//    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-quickcloud/conf/hdfs-site.xml");
    	lstFileConfs.add("/usr/local/hbase_conf/cloud/hbase-site.xml");
    	lstFileConfs.add("/usr/local/hbase_conf/cloud/core-site.xml");
    	lstFileConfs.add("/usr/local/hbase_conf/cloud/hdfs-site.xml");
    	
    	String returnString="";
    	
    	try {
    		connCloud.connectHBase(lstFileConfs);
    		
    		Map<String, Grabacion> mapGrabaciones = new HashMap<>();
    		
    		mapGrabaciones = new TreeMap<>(connCloud.getMapGrabaciones(request));

        	Rutinas mylib = new Rutinas();
        	
        	returnString = mylib.serializeObjectToJSon(mapGrabaciones, false);
    		
        	respuesta = returnString;
    	} catch (Exception e) {
    		respuesta = "Error: "+e.getMessage();
    	}
    	
    	return respuesta;
    }
}
