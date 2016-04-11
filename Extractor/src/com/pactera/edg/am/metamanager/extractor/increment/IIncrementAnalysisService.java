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

package com.pactera.edg.am.metamanager.extractor.increment;

import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;

/**
 * 作增量或全量分析的接口
 * 
 * @author user
 * @version 1.0 Date: Sep 29, 2009
 * 
 */
public interface IIncrementAnalysisService {

	// 全量式比较
	final String FULL_INCREMENT_SPRING_NAME = "fullIncrementAnalysisService";

	// 增量式比较
	final String INCREMENT_SPRING_NAME = "incrementAnalysisService";

	/**
	 * 作增量或全量分析的接口
	 * 
	 * @param srObject
	 *            新的数据
	 * @return 需要添加,修改的元数据
	 * @throws Throwable
	 */
	public Map<Operation, AppendMetadata> incrementAnalysis(AppendMetadata aMetadata) throws Throwable;
}
