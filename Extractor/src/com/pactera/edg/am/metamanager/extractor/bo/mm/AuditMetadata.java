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

package com.pactera.edg.am.metamanager.extractor.bo.mm;

/**
 * 已确认的待审核元数据
 * 
 * @author hqchen
 * @version 1.0 Date: Oct 11, 2009
 */
public class AuditMetadata extends MMMetadata {
	private static final long serialVersionUID = 35345456575676L;

	// 操作类型:1:增加;2:删除;3:修改
	private String operType;

	public String getOperType() {
		return operType;
	}

	public void setOperType(String operType) {
		this.operType = operType;
	}

	public int compareTo(AuditMetadata o) {
		int compareInt = getClassifierId().compareTo(o.getClassifierId());
		if (compareInt == 0) {
			compareInt = operType.compareTo(o.getOperType());
			if (compareInt == 0) {
				compareInt = getId().compareTo(o.getId());
			}
		}
		return compareInt;
	}
}
