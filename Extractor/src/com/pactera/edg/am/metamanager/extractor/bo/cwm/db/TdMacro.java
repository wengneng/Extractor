/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, BeiJing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */


package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.HashMap;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;

public class TdMacro extends ModelElement {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7251804866175819420L;
	
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

}
