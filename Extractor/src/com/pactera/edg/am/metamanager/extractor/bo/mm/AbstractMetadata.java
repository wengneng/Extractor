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


package com.pactera.edg.am.metamanager.extractor.bo.mm;

import java.util.List;
import java.util.Map;

/**
 * 元数据抽象类
 *
 * @author chen hanqing
 * @version 1.0  Date: Oct 2, 2009
 * @author fbchen 2010-01-18 增加UID：业务唯一标识
 */
public abstract class AbstractMetadata {

	/**
	 * 唯一标识ID，如在Erwin采集时XML文件已经为节点定义了一个ID
	 */
	protected String uid;
	
	public abstract void print();
	
	public abstract String getId();
	
	public abstract void setId(String id);
	
	public abstract boolean isHasExist();
	
	public abstract MMMetadata getParentMetadata();
	
	public abstract Map<String, String> getAttrs();
	
	public abstract String getCode();
	
	public abstract String getClassifierId();
	
	public abstract String getNamespace();
	
	public abstract Long getStartTime();
	
	public abstract List<AbstractMetadata> getChildrenMetadatas();

	/**
	 * @return the uid 唯一标识ID
	 */
	public String getUid() {
		return uid;
	}

	/**
	 * @param uid 唯一标识ID
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}
	
}
