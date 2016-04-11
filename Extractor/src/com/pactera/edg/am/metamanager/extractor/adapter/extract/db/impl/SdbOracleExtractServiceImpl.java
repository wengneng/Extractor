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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Partition;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;

/**
 * 深发特有的采集表分区的子类
 * 
 * @author user
 * @version 1.0 Date: Nov 5, 2009
 * 
 */
public class SdbOracleExtractServiceImpl extends OracleExtractServiceImpl {

	private Log log = LogFactory.getLog(SdbOracleExtractServiceImpl.class);

	protected void setPartitions(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集表分区信息...");
		String sql = "SELECT P.TABLE_OWNER, P.TABLE_NAME, P.TABLESPACE_NAME, P.COMPOSITE, P.PARTITION_NAME, "
				+ "P.PARTITION_POSITION FROM ALL_TAB_PARTITIONS P WHERE P.TABLE_OWNER=?";

		super.getJdbcTemplate().query(sql, new Object[] { schName }, new PartitionRowCallbackHandler(tableCache));
	}

	private class PartitionRowCallbackHandler implements RowCallbackHandler {

		private Map<String, Table> tableCache;

		public PartitionRowCallbackHandler(Map<String, Table> tableCache)
		{
			this.tableCache = tableCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String tableName = rs.getString(Table.TABLE_NAME);
			if (tableCache.containsKey(tableName)) {
				Partition partition = new Partition();
				partition.setName(rs.getString(Partition.PARTITION_NAME));
				partition.setTableName(tableName);
				partition.addAttr(Partition.COMPOSITE, rs.getString(Partition.COMPOSITE));
				partition
						.addAttr(Partition.PARTITION_POSITION, String.valueOf(rs.getInt(Partition.PARTITION_POSITION)));
				partition.addAttr(Partition.TABLESPACE_NAME, rs.getString(Partition.TABLESPACE_NAME));

				tableCache.get(tableName).addPartition(partition);
			}

		}

	}

	/**
	 * 设置视图的SQL属性
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl#setViewText(java.lang.String,
	 *      java.util.Map)
	 */
	protected void setViewText(String schName, Map<String, View> viewCache) throws SQLException {
		String sql = "SELECT VIEW_NAME, TEXT AS sql FROM DBA_VIEWS WHERE OWNER = ?";

		super.getJdbcTemplate().query(sql, new Object[] { schName }, new ViewRowCallbackHandler(viewCache));
	}

	private class ViewRowCallbackHandler implements RowCallbackHandler {
		private Map<String, View> viewCache;

		public ViewRowCallbackHandler(Map<String, View> viewCache)
		{
			this.viewCache = viewCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String viewText = rs.getString("VIEW_NAME");
			if (viewCache.containsKey(viewText)) {
				viewCache.get(viewText).addAttr(View.SQL, rs.getString(View.SQL));
			}

		}

	}
}
