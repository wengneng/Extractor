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
package com.pactera.edg.am.metamanager.extractor.dao;

import java.sql.SQLException;

import com.pactera.edg.am.metamanager.app.bo.THarvestDatasource;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorTask;
import com.pactera.edg.am.metamanager.extractor.ex.TaskInstanceNotFoundException;

/**
 * 采集任务相关配置DAO接口
 *
 * @author hqchen
 * @version 1.0  Date: Aug 6, 2009
 */
public interface IExtractorConfDao {
	
	/**
	 * 根据任务ID,获取具体采集任务的配置信息
	 * @param taskId 任务ID
	 * @return 采集任务的配置信息
	 */
	ExtractorTask queryTask(final String taskInstanceId);
	
	/**
	 * 根据任务ID,获取该采集任务所用的数据源配置信息
	 * @param taskId 任务ID
	 * @return 数据源配置信息
	 */
	THarvestDatasource queryDatasource(final String taskInstanceId);

	/**
	 * 获取任务实例的相关信息,并存储于缓存中
	 * @param taskInstanceId 任务实例ID
	 * @throws TaskInstanceNotFoundException 如不存在该任务实例,则抛出该异常
	 */
	void genTaskInstanceConf(String taskInstanceId) throws TaskInstanceNotFoundException, SQLException;
	
	/**
	 * 根据任务实例ID，获取创建者信息
	 * @param taskInstanceId
	 */
	void queryUser(final String taskInstanceId);
}
