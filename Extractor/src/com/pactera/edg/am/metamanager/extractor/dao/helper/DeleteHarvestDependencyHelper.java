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

package com.pactera.edg.am.metamanager.extractor.dao.helper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;

/**
 * 將待刪除的依賴關係寫至待审核表
 * 
 * @author hqchen
 * @version 1.0 Date: Oct 9, 2009
 */
public class DeleteHarvestDependencyHelper extends CreateHarvestDependencyHelper {

	private Log log = LogFactory.getLog(DeleteHarvestDependencyHelper.class);

	public DeleteHarvestDependencyHelper(int batchSize) {
		super(batchSize);
	}

	public void setDeleteDependencies(List<MMDDependency> dependenciesList) {
		this.dependenciesList = dependenciesList;
	}

	protected void setPs(PreparedStatement ps, int index) throws SQLException {
		// 任务实例ID
		ps.setString(index + 1, taskInstanceId);
		// 表示待审核
		ps.setString(index + 2, "0");
		// 表示操作的类型:增加,删除
		ps.setString(index + 3, "2"); //2-删除
	}
	
}
