/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, BeiJing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */


package com.pactera.edg.am.metamanager.extractor.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLog;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 采集日志的观察者
 *
 * @author user
 * @version 1.0  Date: Oct 8, 2009
 *
 */
public class ExtractorLogObservable extends Observable {

	/**
	 * 日志列表缓存
	 */
	private List<ExtractorLog> extractorLogs = new ArrayList<ExtractorLog>();
	
	/**
	 * 日志列表记录数
	 */
	private int count;

	public List<ExtractorLog> getExtractorLogs() {
		return extractorLogs;
	}
	
	private void clear(){
		extractorLogs.clear();
	}

	/**
	 * 添加日志至缓存中,当满足条件时,则触发写日志至数据库
	 * @param log
	 */
	public void addExtractorLog(ExtractorLog log){
		extractorLogs.add(log);
		if(++count % Constants.MAX_EXTRACTORLOG_SIZE == 0){
			triggeRecordLog();
		}
	}
	
	private void triggeRecordLog(){
		super.setChanged();
		super.notifyObservers();
		super.clearChanged();
		clear();
	}
	
	/**
	 * 提供手动触发写日志至数据库
	 */
	public void recordLogManual(){
		triggeRecordLog();
		count = 0;
	}
}
