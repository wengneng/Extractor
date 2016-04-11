package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.ArrayList;
import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.DataType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;

public class Schema extends ModelElement {

	public static final String SCH_NAME = "SCH_NAME";

	private List<CheckConstraint> constraints;

	private List<Procedure> procedures = new ArrayList<Procedure>(0);

	private List<Trigger> triggers = new ArrayList<Trigger>(0);

	private List<NamedColumnSet> columnSets = new ArrayList<NamedColumnSet>();

	// private List<Table> tables = new ArrayList<Table>();
	//	
	// private List<View> views = new ArrayList<View>();

	private List<SQLIndex> SQLIndexs = new ArrayList<SQLIndex>(0);

	private List<DataType> dataTypes = new ArrayList<DataType>(0);

	private List<TdMacro> macros = new ArrayList<TdMacro>(0);

	public List<TdMacro> getMacros() {
		return macros;
	}

	public void setMacros(List<TdMacro> macros) {
		this.macros = macros;
	}

	public List<CheckConstraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(List<CheckConstraint> constraints) {
		this.constraints = constraints;
	}

	public List<Procedure> getProcedures() {
		return procedures;
	}

	public void setProcedures(List<Procedure> procedures) {
		this.procedures = procedures;
	}

	public void addProcedure(Procedure procedure) {
		procedures.add(procedure);
	}

	public List<Trigger> getTriggers() {
		return triggers;
	}

	public void setTriggers(List<Trigger> triggers) {
		this.triggers = triggers;
	}

	public void addTrigger(Trigger trigger) {
		triggers.add(trigger);
	}

	public List<NamedColumnSet> getColumnSets() {
		return columnSets;
	}

	public void setColumnSets(List<NamedColumnSet> columnSets) {
		this.columnSets = columnSets;
	}

	public void addColumnSet(NamedColumnSet columnSet) {
		this.columnSets.add(columnSet);
	}

	public void addColumnSet(List<NamedColumnSet> columnSets) {
		this.columnSets.addAll(columnSets);
	}

	public List<SQLIndex> getSQLIndexs() {
		return SQLIndexs;
	}

	public void setSQLIndexs(List<SQLIndex> indexs) {
		SQLIndexs = indexs;
	}

	public List<DataType> getDataTypes() {
		return dataTypes;
	}

	public void setDataTypes(List<DataType> dataTypes) {
		this.dataTypes = dataTypes;
	}

	// public List<Table> getTables() {
	// return tables;
	// }
	//
	// public void setTables(List<Table> tables) {
	// this.tables = tables;
	// }
	//
	// public List<View> getViews() {
	// return views;
	// }
	//
	// public void setViews(List<View> views) {
	// this.views = views;
	// }

}
