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
package com.pactera.edg.am.metamanager.extractor.control;

import java.sql.SQLException;

import com.pactera.edg.am.metamanager.app.bo.THarvestDatasource;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorTask;
import com.pactera.edg.am.metamanager.extractor.ex.TaskInstanceNotFoundException;

/**
 * 实现从数据库中获取采集模块所需配置信息
 *
 * @author qhchen
 * @version 1.0  Date: Aug 7, 2009
 */
public interface IExtractorConfLoader {

	final String SPRING_NAME = "iExtractorConfLoader";
	
	/**
	 * 根据采集任务ID获取采集任务相关信息
	 * @param taskId 采集任务ID
	 * @return
	 */
	ExtractorTask queryTask(String taskInstanceId);
	
	/**
	 * 根据采集任务ID获取其关联的数据源信息
	 * @param taskId 采集任务ID
	 * @return
	 */
	THarvestDatasource queryDatasource(String taskInstanceId);

	/**
	 * 根据任务实例ID，获取任务实例的配置信息
	 * @param taskInstanceId
	 * @throws TaskInstanceNotFoundException　任务实例不存在
	 */
	void genTaskInstanceConf(String taskInstanceId) throws TaskInstanceNotFoundException, SQLException;
	
	/**
	 * 根据任务实例ID，获取创建者信息
	 * @param taskInstanceId
	 */
	void queryUser(String taskInstanceId);

	void getTargetDBInfo();
}
