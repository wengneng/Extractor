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

package com.pactera.edg.am.metamanager.extractor.dao;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditMetadata;

/**
 * 待审核表操作DAO
 * 
 * @author user
 * @version 1.0 Date: Oct 4, 2009
 * 
 */
public interface IHarvestMetadataDao extends ICreateDao, IDeleteDao {

	/**
	 * 将需要添加的元数据,批量写入待审核表中
	 * 
	 * @param createAMetadata
	 */
	// void batchLoadCreate(final AppendMetadata createAMetadata) ;
	/**
	 * 将需要修改的元数据,批量写入待审核表中
	 * 
	 * @param modifyAMetadata
	 */
	void batchLoadModify(final AppendMetadata modifyAMetadata);

	/**
	 * 将需要删除的元数据,批量写入待审核表中
	 * 
	 * @param deleteAMetadata
	 */
	// void batchLoadDelete(final AppendMetadata deleteAMetadata);
	/**
	 * 根据任务实例ID,获取已确认的待审核表中的元数据列表
	 * 
	 * @return
	 */
	List<AuditMetadata> genAuditAMetadata();

	/**
	 * 非本批次的元数据,如与本批次有相同的数据,则置其状态为撤销
	 */
	void updateOldDatas();
}
