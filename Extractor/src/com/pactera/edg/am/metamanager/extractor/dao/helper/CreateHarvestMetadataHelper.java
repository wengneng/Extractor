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
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 添加待审核元数据操作PreparedStatementCallback辅助类
 * 
 * @author user
 * @version 1.0 Date: Oct 4, 2009
 * 
 */
public class CreateHarvestMetadataHelper extends CreateMetadataHelper {

	// private Log log = LogFactory.getLog(CreateHarvestMetadataHelper.class);

	protected String operType;

	protected String taskInstanceId;

	public CreateHarvestMetadataHelper(int batchSize) {
		super(batchSize);
		taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();
	}

	protected void setPs(PreparedStatement ps, MMMetadata metadata, int index) throws SQLException {
		// START_TIME: 系统时间
		ps.setLong(7, this.getGlobalTime());
		
		// 任务实例ID
		ps.setString(index + 1, taskInstanceId);
		// 表示待审核
		ps.setString(index + 2, "0");
		// 表示操作的类型:增加,修改,删除
		ps.setString(index + 3, operType);
	}
	
	protected Map<String, String> getMetadataAttrPrepared(MMMetadata metadata) {
		return metadata.getAttrs();
	}

	public void setOperType(String operType) {
		this.operType = operType;
	}

}
