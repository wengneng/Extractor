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

import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;

/**
 * 操作元数据变更信息表中,对需修改的元数据,写至元数据变更信息表的PreparedStatementCallback辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Oct 10, 2009
 */
public class ModifyMetadataAlterHelper extends CreateMetadataAlterHelper {

	// private Log log = LogFactory.getLog(ModifyMetadataAlterHelper.class);

	public ModifyMetadataAlterHelper(int batchSize)
	{
		super(batchSize);
	}

	protected void setPs(PreparedStatement ps, AbstractMetadata metadata, String metaModelCode,
			boolean hasChildMetaModel) throws SQLException {
		// 变更类型,修改为2
		ps.setString(2, "2");

		// 命名空间
		String namespace = metadata.getNamespace();
		ps.setString(6, namespace == null ? super.genNamespace(metadata.getParentMetadata(), metadata.getId(),
				hasChildMetaModel) : namespace);
		
		// 元数据的START_TIME
		ps.setLong(8, this.startTime);
		
		// OLD_STARTTIME: 元数据修改之前的Start_Time
		ps.setLong(10, metadata.getStartTime());
	}

}
