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
package com.pactera.edg.am.metamanager.extractor.control.impl;

import java.sql.SQLException;

import com.pactera.edg.am.metamanager.app.bo.THarvestDatasource;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorTask;
import com.pactera.edg.am.metamanager.extractor.control.IExtractorConfLoader;
import com.pactera.edg.am.metamanager.extractor.dao.IExtractorConfDao;
import com.pactera.edg.am.metamanager.extractor.dao.ITargetDBInfoDao;
import com.pactera.edg.am.metamanager.extractor.ex.TaskInstanceNotFoundException;

/**
 * 实现从数据库中获取采集模块所需配置信息
 *
 * @author user
 * @version 1.0  Date: Aug 7, 2009
 */
public class ExtractorConfLoaderImpl implements IExtractorConfLoader {

	/**
	 * 采集器配置信息获取的dao
	 */
	private IExtractorConfDao extractorConfDao;
	
	private ITargetDBInfoDao targetDBInfoDao;

	public void setExtractorConfDao(IExtractorConfDao extractorConfDao) {
		this.extractorConfDao = extractorConfDao;
	}
	
	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorConfLoader#queryTask(String)
	 */
	public ExtractorTask queryTask(String taskInstanceId){
		return extractorConfDao.queryTask(taskInstanceId);
	}
	
	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorConfLoader#queryDatasource(String)
	 */
	public THarvestDatasource queryDatasource(String taskInstanceId){
		return extractorConfDao.queryDatasource(taskInstanceId);
	}

	public void genTaskInstanceConf(String taskInstanceId) throws TaskInstanceNotFoundException, SQLException{
		extractorConfDao.genTaskInstanceConf(taskInstanceId);
		
	}
	
	public void queryUser(String taskInstanceId){
		extractorConfDao.queryUser(taskInstanceId);
	}

	public void getTargetDBInfo() {
		targetDBInfoDao.getTargetDBInfo();
	}

	public void setTargetDBInfoDao(ITargetDBInfoDao targetDBInfoDao) {
		this.targetDBInfoDao = targetDBInfoDao;
	}
}
