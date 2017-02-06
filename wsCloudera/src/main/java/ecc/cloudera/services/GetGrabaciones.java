package ecc.cloudera.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import ecc.cloudera.dataAccess.ClouderaHB;
import ecc.cloudera.model.DataRequest;
import ecc.cloudera.model.Grabacion;
import ecc.cloudera.utiles.Rutinas;

public class GetGrabaciones {
	Rutinas mylib = new Rutinas();
	Map<String, Grabacion> mapGrabacion = new HashMap<>();
	int codeResponse;
	String messageResponse;
	int modulo;
	
	public String getMessageResponse() {
		return this.messageResponse;
	}
	
	public void setMessageResponse(String message) {
		this.messageResponse = message;
	}
	
	public int getCodeResponse() {
		return this.codeResponse;
	}
	
	public void setCodeResponse(int code) {
		this.codeResponse = code;
	}
	
	public Map<String, Grabacion> getMapGrabacion() {
		return this.mapGrabacion;
	}
	
	public void setMapGrabacion(Map<String, Grabacion> mapGrab) {
		this.mapGrabacion = mapGrab;
	}
	
	public void getGrabaciones(String dataInput) {
		
		try {
			
			//Parametros de Entrada
			modulo=1;
			
			DataRequest request = new DataRequest();
			
			System.out.println("Serializando el objeto Request");
			request = (DataRequest) mylib.serializeJSonStringToObject(dataInput, DataRequest.class);
			
			//Conecta hacia HBase
			modulo=2;
			
			System.out.println("Instanciando dataClass ClouderaHB()");
			ClouderaHB connCloud = new ClouderaHB();
			
			
	    	List<String> lstFileConfs = new ArrayList<>();
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-cloudera/conf/hbase-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-cloudera/conf/core-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-cloudera/conf/hdfs-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-quickcloud/conf/hbase-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-quickcloud/conf/core-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-quickcloud/conf/hdfs-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-hwkcluster00/conf/hbase-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-hwkcluster00/conf/core-site.xml");
//	    	lstFileConfs.add("/Users/admin/Documents/Apps/Cluster-hwkcluster00/conf/hdfs-site.xml");
	    	lstFileConfs.add("/usr/local/hbase_conf/cloud/hbase-site.xml");
	    	lstFileConfs.add("/usr/local/hbase_conf/cloud/core-site.xml");
	    	lstFileConfs.add("/usr/local/hbase_conf/cloud/hdfs-site.xml");
	    	
	    	System.out.println("Conectando a ClouderHB");
	    	connCloud.connectHBase(lstFileConfs);
			
	    	//Genera consulta de datos
	    	modulo=3;
	    			
    		Map<String, Grabacion> mapGrabaciones = new HashMap<>();
    		
    		mapGrabaciones = new TreeMap<>(connCloud.getMapGrabaciones(request));
    		
    		setMapGrabacion(mapGrabaciones);
    		
    		//Finaliza Exitoso
    		setCodeResponse(0);
			
			
		} catch (Exception e) {
			switch (modulo) {
			case 1:
				setCodeResponse(1);
				setMessageResponse("Error en parametros de entrada: "+e.getMessage());
				break;
			case 2:
				setCodeResponse(2);
				setMessageResponse("Error conectando a HBase: "+e.getMessage());
				break;
			case 3:
				setCodeResponse(3);
				setMessageResponse("Error generando consulta de datos: "+e.getMessage());
				break;
			default:
				setCodeResponse(99);
				setMessageResponse("Error desconicido: "+e.getMessage());
				break;
			}
		}
	}
}
