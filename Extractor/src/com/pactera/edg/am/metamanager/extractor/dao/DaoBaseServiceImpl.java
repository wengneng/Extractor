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

package com.pactera.edg.am.metamanager.extractor.dao;

import java.util.Iterator;
import java.util.Map;

import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.helper.PreparedStatementCallbackHelper;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

/**
 * 操作数据库的基础类
 * 
 * @author user
 * @version 1.0 Date: Aug 8, 2009
 * 
 */
public class DaoBaseServiceImpl extends JdbcDaoSupport {

	// 批量提交的数量
	private int batchSize;

	/**
	 * PreparedStatementCallback实现的实现抽象类,用于协助各种DAO实现批量处理
	 */
	protected PreparedStatementCallbackHelper helper;

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * 根据SQLID从配置文件中获取SQL
	 * 
	 * @param sqlId
	 * @return SQL
	 */
	protected String getSql(String sqlId) {
		return GenSqlUtil.getSql(sqlId);
	}

	protected String genFullSql(String sql, final MMMetaModel metaModel) {
		Map<String, String> mAttributes = metaModel.getMAttrs();
		StringBuilder keys = new StringBuilder();
		StringBuilder values = new StringBuilder();
		for (Iterator<String> iter = mAttributes.values().iterator(); iter.hasNext();) {
			keys.append(",").append(iter.next());
			values.append(",?");
		}

		String fullSql = sql;
		fullSql = fullSql.replaceAll(Constants.SQL_ATTR_KEYS_FLAG, keys.toString());
		fullSql = fullSql.replaceAll(Constants.SQL_ATTR_VALUES_FLAG, values.toString());

		// 组装SQL
		return fullSql;
	}

	protected String genModifySql(String sql, MMMetaModel metaModel) {
		Map<String, String> mAttributes = metaModel.getMAttrs();
		StringBuilder keyValues = new StringBuilder();
		for (Iterator<String> iter = mAttributes.values().iterator(); iter.hasNext();) {
			keyValues.append(",").append(iter.next()).append("=? ");
		}
		String fullSql = sql;
		fullSql = fullSql.replaceAll(Constants.SQL_ATTR_KEYS_FLAG, keyValues.toString());
		return fullSql;
	}

	protected void clear() {
		helper = null;
	}

}
