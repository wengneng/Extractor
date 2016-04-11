package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Attribute;

public class ForeignKey extends Attribute {

	public static final String FK_NAME = "FK_NAME";

	public static final String FKTABLE_NAME = "FKTABLE_NAME";

	public static final String KEY_SEQ = "key_seq";

	public static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";

	public static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";

	public static final String PKTABLE_NAME = "PKTABLE_NAME";

	public static final String PKTABLE_SCHEM = "PKTABLE_SCHEM";

	// ForeignKey依赖于TableColumn
	String fKColumnName;

	private Table ownerTable;

	private String pKColumnName;
	
	private String pKTableName;
	
	private String pKSchemaName;

	public String getPKTableName() {
		return pKTableName;
	}

	public void setPKTableName(String tableName) {
		pKTableName = tableName;
	}

	public String getPKSchemaName() {
		return pKSchemaName;
	}

	public void setPKSchemaName(String schemaName) {
		pKSchemaName = schemaName;
	}

	public Table getOwnerTable() {
		return ownerTable;
	}

	public void setOwnerTable(Table ownerTable) {
		this.ownerTable = ownerTable;
	}

	public void setFKColumnName(String fKColumnName) {
		this.fKColumnName = fKColumnName;
	}

	public String getFKColumnName() {
		return fKColumnName;
	}

	public void setPKColumnName(String pKColumnName) {
		this.pKColumnName = pKColumnName;
		
	}

	public String getPKColumnName() {
		return pKColumnName;
	}
}
