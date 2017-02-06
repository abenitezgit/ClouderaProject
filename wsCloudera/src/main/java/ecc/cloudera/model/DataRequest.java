package ecc.cloudera.model;

public class DataRequest {
	String skill;
	String fechaDesde; //Formato YYYYMMDDHHMISS
	String fechaHasta; //Formato YYYYMMDDHHMISS
	String tableName;
	int limit;   //Limite de registros a retornar 0: unlimited
	
	//Getter ans Setter
	
	public String getSkill() {
		return skill;
	}
	public void setSkill(String skill) {
		this.skill = skill;
	}
	public String getFechaDesde() {
		return fechaDesde;
	}
	public void setFechaDesde(String fechaDesde) {
		this.fechaDesde = fechaDesde;
	}
	public String getFechaHasta() {
		return fechaHasta;
	}
	public void setFechaHasta(String fechaHasta) {
		this.fechaHasta = fechaHasta;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public int getLimit() {
		return limit;
	}
	public void setLimit(int limit) {
		this.limit = limit;
	}
}
