package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;

public class Trigger extends ModelElement {

	public static final String TRIGGER_NAME = "TRIGGER_NAME";
	public static final String TEXT = "TEXT";
	
	// Trigger依赖于Table
	private List<Table> tables;

	private Schema ownerSchema;

	private String remarks;
	
	private String text;

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public Schema getOwnerSchema() {
		return ownerSchema;
	}

	public void setOwnerSchema(Schema ownerSchema) {
		this.ownerSchema = ownerSchema;
	}

	public List<Table> getTables() {
		return tables;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
}
