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

package com.pactera.edg.am.metamanager.extractor.dao.impl;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.dao.ITargetDBInfoDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

public class TargetDBInfoDaoImpl extends JdbcDaoSupport implements ITargetDBInfoDao {

	private Log log = LogFactory.getLog(TargetDBInfoDaoImpl.class);

	public void getTargetDBInfo() {
		log.info("从存储库中获取允许存储的最大长度信息(包括元数据代码,名称,属性及写日志的长度).");
		try {
			BasicDataSource ds = (BasicDataSource) super.getJdbcTemplate().getDataSource();
			DatabaseMetaData metaData = ds.getConnection().getMetaData();

			int maxMetadataCodeSize = getColumnSize(metaData, ds.getUsername().toUpperCase(), "T_MD_INSTANCE",
					"INSTANCE_CODE");
			if (maxMetadataCodeSize != -1)
				AdapterExtractorContext.getInstance().setMaxMetadataCodeSize(maxMetadataCodeSize);

			int maxMetadataNameSize = getColumnSize(metaData, ds.getUsername().toUpperCase(), "T_MD_INSTANCE",
					"INSTANCE_NAME");
			if (maxMetadataNameSize != -1)
				AdapterExtractorContext.getInstance().setMaxMetadataNameSize(maxMetadataNameSize);

			int maxMetadataAttrSize = getColumnSize(metaData, ds.getUsername().toUpperCase(), "T_MD_INSTANCE",
					"STRING_3");
			if (maxMetadataAttrSize != -1)
				AdapterExtractorContext.getInstance().setMaxMetadataAttrSize(maxMetadataAttrSize);

			int maxLogSize = getColumnSize(metaData, ds.getUsername().toUpperCase(), "T_TASK_INSTANCE_DESC",
					"DESCRIPTION");
			if (maxLogSize != -1)
				AdapterExtractorContext.getInstance().setMaxLogSize(maxLogSize);

			// -----------------------------------
			print();
		}
		catch (SQLException e) {
			log.error("从目标库中获取字段大小失败，将使用默认值．", e);
		}
	}

	private void print() {
		log.info("存储元数据代码的最大长度:" + AdapterExtractorContext.getInstance().getMaxMetadataCodeSize());
		log.info("存储元数据名称的最大长度:" + AdapterExtractorContext.getInstance().getMaxMetadataNameSize());
		log.info("存储元数据属性的最大长度:" + AdapterExtractorContext.getInstance().getMaxMetadataAttrSize());
		log.info("存储元数据日志的最大长度:" + AdapterExtractorContext.getInstance().getMaxLogSize());
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "存储元数据代码的最大长度:"
				+ AdapterExtractorContext.getInstance().getMaxMetadataCodeSize());
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "存储元数据名称的最大长度:"
				+ AdapterExtractorContext.getInstance().getMaxMetadataNameSize());
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "存储元数据属性的最大长度:"
				+ AdapterExtractorContext.getInstance().getMaxMetadataAttrSize());
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "存储元数据日志的最大长度:"
				+ AdapterExtractorContext.getInstance().getMaxLogSize());
	}

	private int getColumnSize(DatabaseMetaData metaData, String schema, String table, String column)
			throws SQLException {
		ResultSet rs = metaData.getColumns(null, schema, table, column);
		int columnSize = -1;
		while (rs.next()) {
			columnSize = rs.getInt(Column.COLUMN_SIZE) / 3;
		}
		rs.close();
		return columnSize;
	}

}
