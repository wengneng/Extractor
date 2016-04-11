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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db;

import java.sql.SQLException;
import java.util.List;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SystemVO;

/**
 * 元数据定制版DB适配器
 * 
 * @author xks
 * @version 1.0 Date:  2012
 * 
 */
public interface IJiCaiDBExtractService {

	/** 通过采集DB直连采集数据字典
	 * @return
	 * @throws SQLException
	 */
	SystemVO getSysInfos(String schema,int start,int limit,boolean flag) throws Exception;

	/**
	 * 获取表级长度信息
	  * @Title: getTabSize  
	  * @Description: TODO 
	  * @param @return 
	  * @return int 
	  * @throws
	 */
	int getTableSize(String schema) throws SQLException;

	/**
	 * 获取字段长度信息
	  * @Title: getFieSize  
	  * @Description: TODO 
	  * @param @return 
	  * @return int 
	  * @throws
	 */
	int getFieldSize(String schema) throws SQLException;
	
	/**
	 * 初始化hsqldb数据库
	  * @Title: initHSQLDB  
	  * @Description: TODO 
	  * @param @param schName
	  * @param @throws Exception 
	  * @return void 
	  * @throws
	 */
	void initHSQLDB(String schName) throws  Exception;
}
