package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;


public class IndexedFeature extends ModelElement {

	// IndexedFeature依赖于TableColumn
	private List<Column> columns;

	private SQLIndex ownerSQLIndex;

	public SQLIndex getOwnerSQLIndex() {
		return ownerSQLIndex;
	}

	public void setOwnerSQLIndex(SQLIndex ownerSQLIndex) {
		this.ownerSQLIndex = ownerSQLIndex;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
}
