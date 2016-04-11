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

/**
 * 已确认的待审核元数据依赖关系
 *
 * @author user
 * @version 1.0  Date: Oct 11, 2009
 *
 */
public class AuditDependency extends MMDDependency {

	/**
	 * 
	 */
	private static final long serialVersionUID = 434324243234L;
	
	private String type;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
