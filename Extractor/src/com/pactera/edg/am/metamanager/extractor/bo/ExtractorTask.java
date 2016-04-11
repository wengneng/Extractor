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

package com.pactera.edg.am.metamanager.extractor.bo;

import java.io.Serializable;

/**
 * 任务实例 
 *
 * @author hqchen
 * @version 1.0  Date: Aug 6, 2009
 */
public class ExtractorTask implements Serializable {
	private static final long serialVersionUID = 23465768687L;
	
	private String taskInstanceId;
	
	private String name;
	
	private String description;
	
	private String datasourceId;
	
	public ExtractorTask(){
		
	}
	
	public ExtractorTask(String taskInstanceId){
		this.taskInstanceId = taskInstanceId;
	}

	public String getTaskInstanceId() {
		return taskInstanceId;
	}

	public void setTaskInstanceId(String taskInstanceId) {
		this.taskInstanceId = taskInstanceId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDatasourceId() {
		return datasourceId;
	}

	public void setDatasourceId(String datasourceId) {
		this.datasourceId = datasourceId;
	}
	
	

}
