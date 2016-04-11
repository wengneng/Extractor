package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.HashMap;
import java.util.Map;

public class View extends NamedColumnSet {
	private static final long serialVersionUID = -8254895871319671636L;

	public final static String SQL = "SQL";

	public final static String REFERENCED_TYPE = "REFERENCED_TYPE";
	
	public final static String VIEW_NAME = "VIEW_NAME";

	/**
	 * <schema.table, NamedColumnSetType>
	 */
	private Map<String, NamedColumnSetType> referenceSchTables = new HashMap<String, NamedColumnSetType>(1);

	public Map<String, NamedColumnSetType> getReferenceSchTables() {
		return referenceSchTables;
	}

	public void addReferenceSchTable(String schTableName, NamedColumnSetType type) {
		referenceSchTables.put(schTableName, type);
	}

	public int compareTo(NamedColumnSet o) {
		return super.getName().compareTo(o.getName());
	}
}
