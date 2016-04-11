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


package com.pactera.edg.am.metamanager.extractor.dao;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;

/**
 * 元数据变更信息表DAO接口
 *
 * @author user
 * @version 1.0  Date: Oct 9, 2009
 *
 */
public interface IMetadataAlterDao extends IRollbackDao{
	
	public static final String SPRING_NAME = "iMetadataAlterDao";

	/**
	 * 将需要添加的元数据,批量写入至变更信息表
	 * @param createAMetadata
	 */
	void alterBatchCreate(AppendMetadata createAMetadata);

	/**
	 * 将需要修改的元数据,批量写入至变更信息表
	 * @param modifyAMetadata
	 */
	void alterBatchModify(AppendMetadata modifyAMetadata);

	/**
	 * 将需要删除的元数据,批量写入至变更信息表
	 * @param deleteAMetadata
	 */
	void alterBatchDelete(AppendMetadata deleteAMetadata);
}
