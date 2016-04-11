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

package com.pactera.edg.am.metamanager.extractor.bo;

import java.io.Serializable;

/**
 * 采集日志
 * 
 * @author chen hanqing
 * @version 1.0 Date: Oct 8, 2009
 */
public class ExtractorLog implements Serializable {

	private static final long serialVersionUID = 34095850980L;

	/**
	 * 创建采集/其他日志
	 * @param level 日志级别
	 * @param message 日志内容
	 * @param logType 日志类型
	 */
	public ExtractorLog(ExtractorLogLevel level, LogType logType, String message) {
		this.level = level;
		this.message = message;
		this.logType = logType.getType();
		this.logTime = System.currentTimeMillis();
	}
	
	/**
	 * 创建采集日志
	 * @param level 日志级别
	 * @param message 日志内容
	 */
	public ExtractorLog(ExtractorLogLevel level, String message) {
		this(level, LogType.EXTRACTOR, message);
	}
	

	// 日志记录的级别
	private ExtractorLogLevel level;

	// 日志记录时间
	private long logTime;

	// 日志信息
	private String message;
	
	// 日志类型
	private String logType;

	public ExtractorLogLevel getLevel() {
		return level;
	}

	public long getLogTime() {
		return logTime;
	}

	public String getMessage() {
		return message;
	}
	
	/**
	 * @return the logType
	 */
	public String getLogType() {
		return logType;
	}

	/**
	 * @param logType the logType to set
	 */
	public void setLogType(String logType) {
		this.logType = logType;
	}


	/**
	 * 日志类型
	 * @author fbchen
	 * @version 2.2 2011-01-12
	 */
	public static enum LogType {
		// 采集
		EXTRACTOR("1"),
		// 审核
		APPROVAL("2"),
		// SQL解析
		SQLPARSER("3");
		
		private String type;
		
		private LogType(String type) {
			this.type = type;
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}
	}

}
