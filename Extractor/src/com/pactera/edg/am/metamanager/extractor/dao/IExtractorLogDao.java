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

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLog;

/**
 * 采集的写日志DAO
 * 
 * @author user
 * @version 1.0 Date: Oct 8, 2009
 * 
 */
public interface IExtractorLogDao {

	public final static String SPRING_NAME = "iExtractorLogDao";

	/**
	 * 批量写日志
	 * 
	 * @param extractorLogs
	 */
	void batchCreate(List<ExtractorLog> extractorLogs);

}
