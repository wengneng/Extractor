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
 * 修改的属性
 *
 * @author user
 * @version 1.0  Date: Oct 2, 2009
 *
 */
public class ModifyAttribute {

	/**
	 * 属性key
	 */
	private String key;
	
	/**
	 * 属性原有值
	 */
	private String oldValue;
	
	/**
	 * 属性新值
	 */
	private String newValue;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getOldValue() {
		return oldValue;
	}

	public void setOldValue(String oldValue) {
		this.oldValue = oldValue;
	}

	public String getNewValue() {
		return newValue;
	}

	public void setNewValue(String newValue) {
		this.newValue = newValue;
	}
}
