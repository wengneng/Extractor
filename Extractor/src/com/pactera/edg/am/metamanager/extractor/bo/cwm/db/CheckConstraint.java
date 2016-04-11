package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Constraint;


public class CheckConstraint extends Constraint {

	private Schema ownerSchema;

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

	// CheckConstraint依赖于Table
	private List<Table> tables;

	private List<ConstraintColumn> constraintColumns;

	public List<ConstraintColumn> getConstraintColumns() {
		return constraintColumns;
	}

	public void setConstraintColumns(List<ConstraintColumn> constraintColumns) {
		this.constraintColumns = constraintColumns;
	}
}
