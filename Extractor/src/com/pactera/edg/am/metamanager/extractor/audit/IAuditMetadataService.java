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

package com.pactera.edg.am.metamanager.extractor.audit;

import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.ex.MetaModelNotFoundException;

/**
 * 待审核元数据操作接口
 * 
 * @author user
 * @version 1.0 Date: Oct 11, 2009
 * 
 */
public interface IAuditMetadataService {

	public static final String SPRING_NAME = "iAuditMetadataService";
	/**
	 * 获取需要添加,修改,删除的待审核元数据,依赖关系
	 * @return
	 * @throws MetaModelNotFoundException
	 */
	Map<Operation, AppendMetadata> genAuditAMetadata() throws MetaModelNotFoundException;
}
