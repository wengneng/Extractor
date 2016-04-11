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
import com.pactera.edg.am.metamanager.extractor.ex.DataRollbackException;

/**
 * 组合关系操作DAO,包括添加,修改,删除,都作批量操作
 * 组合关系批量添加:遍历所有需要添加的元数据,得到它们之间的组合关系,当记录数等于批量提交记录数的倍数时提交添加记录,最后提交记录数的尾数
 * 组合关系批量删除同组合关系批量添加
 * 
 * @author user
 * @version 1.0 Date: Jul 23, 2009
 * 
 */
public interface ICompositionDao extends IDeleteDao {

	/**
	 * 批量添加元数据组合关系
	 * 
	 * @param createAMetadata
	 */
	void batchLoadCreate(AppendMetadata createAMetadata) throws Exception, DataRollbackException;
	/**
	 * 批量删除元数据组合关系
	 * 
	 * @param deleteAMetadata
	 */
	// void batchLoadDelete(AppendMetadata deleteAMetadata);
}
