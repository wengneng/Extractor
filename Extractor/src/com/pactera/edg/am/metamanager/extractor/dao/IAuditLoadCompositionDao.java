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

/**
 * 对已确认的待审核元数据的的组合关系DAO操作(不作事务控制)
 * 
 * @author user
 * @version 1.0 Date: Oct 13, 2009
 * 
 */
public interface IAuditLoadCompositionDao {

	/**
	 * 审核入库需添加的元数据组合关系
	 */
	void auditLoadCreate();

	/**
	 * 审核入库需删除的元数据组合关系
	 */
	void auditLoadDelete();

	/**
	 * 删除本次任务实例的所有待审核组合关系
	 */
	void delete();

}
