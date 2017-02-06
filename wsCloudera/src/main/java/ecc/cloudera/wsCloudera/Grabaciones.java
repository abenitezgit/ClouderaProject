package ecc.cloudera.wsCloudera;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import ecc.cloudera.dataAccess.ClouderaHB;
import ecc.cloudera.model.DataRequest;
import ecc.cloudera.model.Grabacion;
import ecc.cloudera.services.GetGrabaciones;
import ecc.cloudera.utiles.Rutinas;

@Path("grabaciones")
public class Grabaciones {
	Rutinas mylib = new Rutinas();
	
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

	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public String grabaciones(String dataInput) {
		try {
			System.out.println("Inicio POST");
			System.out.println("Datos de entrada: "+dataInput);
			GetGrabaciones getGrab = new GetGrabaciones();
			
			//Genera consulta de grabaciones con los parametros recibidos 
			//desde dataInput en formato String(JSON)
			
			getGrab.getGrabaciones(dataInput);
			
			if (getGrab.getCodeResponse()==0) {
				if (getGrab.getMapGrabacion().size()>0) {
					String stringMap = mylib.serializeObjectToJSon(getGrab.getMapGrabacion(), false);
				
					//return Response.status(200).entity(stringMap).build();
					return stringMap;
				} else {
					//return Response.status(200).entity("No hay grabaciones").build();
					return "no hay grabaciones";
				}
			} else {
				//return Response.status(401).entity(getGrab.getMessageResponse()).build();
				return "error";
			}
			
		} catch (Exception e) {
			//return Response.status(500).entity("Error desconocido: "+e.getMessage()).build();
			return "error";
		}
	}
	
}
