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


package com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class DeleteDataBo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1125850575945900736L;
	
	// 删除的元模型
	private String classifierId;
	
	// 删除的元模型所对应的SHEET
	private String sheetName;
	
	private int dStart, dEnd;
	
	private Map<String, int[]> pathPosition = new LinkedHashMap<String, int[]>();

	public String getClassifierId() {
		return classifierId;
	}

	public void setClassifierId(String classifierId) {
		this.classifierId = classifierId;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public int getDStart() {
		return dStart;
	}

	public void setDStart(int start) {
		dStart = start;
	}

	public int getDEnd() {
		return dEnd;
	}

	public void setDEnd(int end) {
		dEnd = end;
	}

	public Map<String, int[]> getPathPosition() {
		return pathPosition;
	}

	public void setPathPosition(Map<String, int[]> pathPosition) {
		this.pathPosition = pathPosition;
	} 

}
