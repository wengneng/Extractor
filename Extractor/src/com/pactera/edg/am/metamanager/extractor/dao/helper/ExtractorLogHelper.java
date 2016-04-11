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

package com.pactera.edg.am.metamanager.extractor.dao.helper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.dao.DataAccessException;

import com.pactera.edg.am.metamanager.app.bo.TaskInstance;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLog;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 写日志功能
 * 
 * @author user
 * @version 1.0 Date: Oct 8, 2009
 * 
 */
public class ExtractorLogHelper extends PreparedStatementCallbackHelper {

	private List<ExtractorLog> extractorLogs;

	public ExtractorLogHelper(int batchSize)
	{
		super(batchSize);
	}

	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
		String taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();

		for (ExtractorLog extractorLog : extractorLogs) {
			ExtractorLogLevel level = extractorLog.getLevel();
			if (!TaskInstance.STATE_IMPORTED_ERROR.equals(AdapterExtractorContext.getInstance().getReturnStatus())) {
				// 如果返回状态为error，则不用重新设置了
				if (level == ExtractorLogLevel.ERROR) {
					AdapterExtractorContext.getInstance().setReturnStatus(TaskInstance.STATE_IMPORTED_ERROR);
				}
				else if (level == ExtractorLogLevel.WARN) {
					AdapterExtractorContext.getInstance().setReturnStatus(TaskInstance.STATE_IMPORTED_WARN);
				}
			}
			if(extractorLog.getMessage() == null)	// 日志内容为空
				continue;

			ps.setString(1, taskInstanceId);
			ps.setString(2, String.valueOf(level.ordinal()));
			ps.setLong(3, extractorLog.getLogTime());
			// 对元数据采集日志作截断
			boolean msgTooLength = extractorLog.getMessage().length() > AdapterExtractorContext.getInstance()
					.getMaxLogSize();
			String message = msgTooLength ? extractorLog.getMessage().substring(0,
					AdapterExtractorContext.getInstance().getMaxLogSize()) : extractorLog.getMessage();
			ps.setString(4, message);
			ps.setString(5, extractorLog.getLogType());

			ps.addBatch();
			ps.clearParameters();
		}

		ps.executeBatch();
		ps.clearBatch();

		return null;
	}

	public void setExtractorLogs(List<ExtractorLog> extractorLogs) {
		this.extractorLogs = extractorLogs;

	}

}
