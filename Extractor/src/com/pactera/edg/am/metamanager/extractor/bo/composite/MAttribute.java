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

/**
 * 元模型的属性
 *
 * @author user
 * @version 1.0  Date: Jul 20, 2009
 *
 */
public class MAttribute {

	// 元模型属性名
	private String name;
	
	// 该元数据属性在元数据表中的存储位置
	private String storePosition;
	
	// 存储的数据类型
	private int dataType;
	
	private int positionInSql;

	public int getPositionInSql() {
		return positionInSql;
	}

	public void setPositionInSql(int positionInSql) {
		this.positionInSql = positionInSql;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStorePosition() {
		return storePosition;
	}

	public void setStorePosition(String storePosition) {
		this.storePosition = storePosition;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}
}
