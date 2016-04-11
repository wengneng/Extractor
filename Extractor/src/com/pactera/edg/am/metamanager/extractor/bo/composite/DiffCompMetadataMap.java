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


package com.pactera.edg.am.metamanager.extractor.bo.composite;

import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;

/**
 * (简单概要描述此类完成的功能) 
 *
 * @author user
 * @version 1.0  Date: Jul 20, 2009
 *
 */
public class DiffCompMetadataMap {

	private Map<Operation, CompMetadataMap> compMetadatas;

	public Map<Operation, CompMetadataMap> getCompMetadatas() {
		return compMetadatas;
	}

	public void setCompMetadatas(Map<Operation, CompMetadataMap> compMetadatas) {
		this.compMetadatas = compMetadatas;
	}
}
