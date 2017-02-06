package ecc.cloudera.dataAccess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import ecc.cloudera.model.DataRequest;
import ecc.cloudera.model.Grabacion;

public class ClouderaHB {
	
	Configuration config = HBaseConfiguration.create();
	
	public void connectHBase(List<String> lstFileConfs) throws Exception {
		
		for (int i=0; i<lstFileConfs.size(); i++) {
			config.addResource(new Path(lstFileConfs.get(i)));
		}
		
		//Agregados manualmente
		//config.addResource(new Path("/Users/admin/Documents/Apps/Cluster-cloudera/conf/hbase-site.xml"));
		//config.addResource(new Path("/Users/admin/Documents/Apps/Cluster-cloudera/conf/core-site.xml"));
		//config.addResource(new Path("/Users/admin/Documents/Apps/Cluster-cloudera/conf/hdfs-site.xml"));
	}
	
	public boolean isExistTableName(String tableName) throws Exception {
		
		HBaseAdmin admin = new HBaseAdmin(config);
		boolean isExist = admin.tableExists(tableName);
		admin.close();
		
		return isExist;
	}
	
	
	public List<String> getColumnsNames (String tableName) throws Exception{
		
		List<String> lstColNames = new ArrayList<>();
		
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));
		
		HColumnDescriptor[] cf = tableDescriptor.getColumnFamilies();
		
		for (int i=0; i<cf.length; i++) {
			lstColNames.add(cf[i].getNameAsString());
		}
		
		admin.close();
		
		return lstColNames;
	}
	
	public Map<String, Grabacion> getMapGrabaciones (DataRequest request) throws Exception{
		
		//Ejemplo SKILL: SKILL_103EPH
		
		Map<String, Grabacion> mapGrabacion = new HashMap<>();
		
		System.out.println("Estableciendo conexion pool");
        HTablePool pool = new HTablePool(config,100000);
        //HTableInterface hTable = pool.getTable(request.getTableName());
        HTableInterface hTable = pool.getTable("grabaciones");
        
        Scan scan = new Scan();
        scan.setCaching(10000);

        System.out.println("Estableciendo Start y Stop Rows");
//        scan.setStartRow(Bytes.toBytes(request.getSkill()+"+"+request.getFechaDesde()));
//        scan.setStopRow(Bytes.toBytes(request.getSkill()+"+"+request.getFechaHasta()));
        scan.setStartRow(Bytes.toBytes("SKILL_103EPH+20170112"));
        scan.setStopRow(Bytes.toBytes("SKILL_103EPH+20170113"));
        
        System.out.println("Llenando el scanner");
        ResultScanner scanner = hTable.getScanner(scan);

        Result result;
        
        int rowIdx=0;
        String key;
        String grabName;
        String urlFull;
        String fecha;
        Grabacion myGrab;
        
		try {
			System.out.println("Recorriendo el scanner");
			while((result=scanner.next())!=null && ++rowIdx<request.getLimit()) {
				key = Bytes.toString(result.getRow());
				grabName = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("17")));
				urlFull = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("29")));
				fecha = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("08")));
				
				myGrab = new Grabacion();
				myGrab.setKey(key);
				myGrab.setGrabName(grabName);
				myGrab.setUrlFull(urlFull);
				myGrab.setFecha(fecha);
				
				mapGrabacion.put(key, myGrab);
				System.out.println("insertando key: "+key);
			}
			System.out.println("Terminando exitoso");
		} catch (Exception e)  {
			System.out.println("Error de extraccion");
			new Exception (e);
		}

		System.out.println("Cerrando conexiones");
		pool.close();
		hTable.close();
		scanner.close();
		
		System.out.println("Retornando mapeo de grabaciones");
		return mapGrabacion;
	}
	

}
