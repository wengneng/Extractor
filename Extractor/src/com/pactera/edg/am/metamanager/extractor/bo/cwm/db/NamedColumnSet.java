package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.ArrayList;
import java.util.List;

public abstract class NamedColumnSet extends ColumnSet implements Comparable<NamedColumnSet>{

	private NamedColumnSetType type;

	// properties
	private String tableType;

	private List<Column> columns = new ArrayList<Column>();

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public void addColumn(Column column) {
		columns.add(column);

	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public NamedColumnSetType getType() {
		return type;
	}

	public void setType(NamedColumnSetType type) {
		this.type = type;
	}
}
