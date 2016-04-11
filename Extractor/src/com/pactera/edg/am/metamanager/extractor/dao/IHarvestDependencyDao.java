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

import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditDependency;

/**
 * 待审核依赖关系操作DAO
 * 
 * @author user
 * @version 1.0 Date: Oct 5, 2009
 * 
 */
public interface IHarvestDependencyDao extends ICreateDao, IDeleteDao {

	/**
	 * 将需添加的元数据依赖关系,批量写至待审核元数据表中
	 * 
	 * @param createAMetadata
	 */
	// void batchLoadCreate(AppendMetadata createAMetadata);
	/**
	 * 将需删除的元数据依赖关系,批量写至待审核元数据表中
	 * 
	 * @param deleteAMetadata
	 */
	// void batchLoadDelete(AppendMetadata deleteAMetadata);
	/**
	 * 根据任务实例ID,获取待审核表中所有已确认的元数据依赖关系
	 * 
	 * @return
	 */
	List<AuditDependency> genAuditDependencies();

	/**
	 * 非本批次的依赖关系,如与本批次有相同的数据,则置其状态为撤销
	 */
	void updateOldDatas();
}
