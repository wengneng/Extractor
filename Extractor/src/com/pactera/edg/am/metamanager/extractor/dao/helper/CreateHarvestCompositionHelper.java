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

import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 添加待审核元数据组合关系操作PreparedStatementCallback辅助类
 * 
 * @author user
 * @version 1.0 Date: Oct 5, 2009
 * 
 */
public class CreateHarvestCompositionHelper extends CreateCompositionHelper {
	
	private String taskInstanceId;

	public CreateHarvestCompositionHelper(int batchSize)
	{
		super(batchSize);
		taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();
	}

	protected void setPs(PreparedStatement ps, int index) throws SQLException {
		// 任务实例ID
		ps.setString(index + 1, taskInstanceId);
		// 表示待审核
		ps.setString(index + 2, "0");
		// 表示操作的类型:增加,修改,删除
		ps.setString(index + 3, "1");
	}
	
	protected String getMsg(){
		return "添加元数据待审核组合关系记录数:";
	}

}
