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

import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;

/**
 * 对已确认的待审核元数据的入库DAO操作(不作事务控制)
 * 
 * @author user
 * @version 1.0 Date: Oct 12, 2009
 * 
 */
public interface IAuditLoadMetadataDao {

	/**
	 * 对已确认的待审核元数据中,需添加的元数据,批量添加至元数据表中
	 * 
	 * @param createAMetadata
	 */
	void auditLoadCreate(final AppendMetadata createAMetadata);

	/**
	 * 对已确认的待审核元数据中,需修改的元数据,批量修改至元数据表中
	 * 
	 * @param modifyAMetadata
	 */
	void auditLoadModify(final AppendMetadata modifyAMetadata);

	/**
	 * 删除已本次任务实例的待审核表中的所有元数据记录
	 */
	void delete();

}
