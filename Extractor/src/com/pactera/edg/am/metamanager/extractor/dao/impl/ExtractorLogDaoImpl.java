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

package com.pactera.edg.am.metamanager.extractor.dao.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLog;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IExtractorLogDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.ExtractorLogHelper;

/**
 * 采集日志DAO实现类
 * 
 * @author user
 * @version 1.0 Date: Oct 8, 2009
 * 
 */
public class ExtractorLogDaoImpl extends DaoBaseServiceImpl implements IExtractorLogDao {

	private Log log = LogFactory.getLog(ExtractorLogDaoImpl.class);
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IExtractorLogDao#batchCreate(java.util.List)
	 */
	public void batchCreate(List<ExtractorLog> extractorLogs) {
		// 任务实例ID
		try {
			String sql = super.getSql("LOAD_CREATE_EXTRACTORLOG");

			helper = new ExtractorLogHelper(super.getBatchSize());
			((ExtractorLogHelper) helper).setExtractorLogs(extractorLogs);
			super.getJdbcTemplate().execute(sql, helper);
		}
		catch (Exception e) {
			log.warn("写日志出现异常!",e);
		}
		finally {
			super.clear();
		}

	}
}
