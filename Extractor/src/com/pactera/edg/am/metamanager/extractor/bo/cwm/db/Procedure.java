package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.ArrayList;
import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;

public class Procedure extends ModelElement {

	public static final String PROCEDURE_NAME = "PROCEDURE_NAME";

	public static final String PROCEDURE_TYPE = "procedure_type";

	public static final String TEXT = "text";
	
	private StringBuilder text = new StringBuilder(0);

	private Schema ownerSchema;

	private List<ProcedureColumn> pColumns = new ArrayList<ProcedureColumn>(0);

	private int procedureType;

	public Schema getOwnerSchema() {
		return ownerSchema;
	}

	public void setOwnerSchema(Schema ownerSchema) {
		this.ownerSchema = ownerSchema;
	}

	public int getProcedureType() {
		return procedureType;
	}

	public void setProcedureType(int procedureType) {
		this.procedureType = procedureType;
	}

	public List<ProcedureColumn> getPColumns() {
		return pColumns;
	}

	public void setPColumns(List<ProcedureColumn> columns) {
		pColumns = columns;
	}

	public void addPColumn(ProcedureColumn pColumn) {
		pColumns.add(pColumn);
	}

	public String getText() {
		return text.toString();
	}
	
	public void setText(String text){
		this.text.append(text);
	}
}
