/*
 * Copyright 2009 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, BeiJing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;

/**
 * (简单概要描述此类完成的功能)
 * 
 * @author user
 * @version 1.0 Date: Nov 5, 2009
 * 
 */
public class Partition extends ModelElement {

	public static final String PARTITION_NAME = "PARTITION_NAME";

	public static final String COMPOSITE = "composite";

	public static final String PARTITION_POSITION = "partition_position";

	public static final String TABLESPACE_NAME = "tablespace_name";

	private String schemaName;

	private String tableName;

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
