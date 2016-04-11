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

import java.util.List;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * MM库表字典操作DAO接口
 * 
 * @author user
 * @version 1.0 Date: Oct 16, 2009
 * 
 */
public interface IDBDictionaryDao {

	public static final String SPRING_NAME = "dbDictionaryDao";

	/**
	 * 从MM库表中获取tables的字段,并保持字段在创建时的先后顺序
	 * 
	 * @param tables
	 *            表名列表,格式:(dbname.)tablename
	 * @return Map<dbname.tablename, List<Map<COLUMNNAME, columnname>>>
	 */
	Map<String, List<Map<String, String>>> getColInfoFromMM(List<String> tables);

	/**
	 * 获取参数中的表的元模型(模型中包含了属于表的元数据,字段的元模型,属于字段的元数据)
	 * 
	 * @param tables
	 * @return
	 */
	MMMetaModel getTableMetaModel(List<String> tables);

	/**
	 * 获取源表名在元数据存储库中的名称
	 * 
	 * @param tableName
	 * @return
	 */
	String getTabOriName(String tableName);

	/**
	 * 获取源表名列表在元数据存储库中的名称
	 * 
	 * @param tableNames
	 * @return
	 */
	Map<String, String> getTabOriNames(List<String> tableNames);

	/**
	 * 获取元数据的id
	 * 
	 * @return
	 */
	Map<String, MMMetadata> getMdIdCache();
}
