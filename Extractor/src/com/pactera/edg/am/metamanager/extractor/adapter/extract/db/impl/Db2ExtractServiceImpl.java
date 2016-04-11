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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.PrimaryKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;

/**
 * DB2采集类
 * 
 * @author user
 * @version 1.0 Date: Oct 21, 2010
 * 
 */
public class Db2ExtractServiceImpl extends DBExtractBaseServiceImpl {

	private Log log = LogFactory.getLog(Db2ExtractServiceImpl.class);

	private final static List<String> systemTableList = new ArrayList<String>(9);
	{
		systemTableList.add("SYSCAT.TABLES");
		systemTableList.add("SYSCAT.INDEXES");
		systemTableList.add("SYSCAT.COLUMNS");
		systemTableList.add("SYSCAT.FUNCTIONS");

		systemTableList.add("SYSCAT.PROCEDURES");
		systemTableList.add("SYSCAT.TABCONST");
		systemTableList.add("SYSCAT.KEYCOLUSE");
		systemTableList.add("SYSCAT.INDEXCOLUSE");

		systemTableList.add("SYSIBM.SYSVIEWDEP");
	}

	@Override
	protected List<String> getSystemTableList() {
		return systemTableList;
	}

	@Override
	protected String signOfNoPrivilege() {
		return "表或视图不存在";
	}

	/**
	 * 
	 * @param schName
	 * @param tableCache
	 * @throws SQLException
	 */
	protected void setPrimaryKeies(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集DB2:" + schName + "的主键信息...");
		String sql = "SELECT p.tabname TABLE_NAME, p.constname PK_NAME, k.colname COLUMN_NAME, k.colseq KEY_SEQ FROM "
				+ "syscat.tabconst p, syscat.keycoluse k  WHERE p.tabschema=? AND p.type='P' AND p.constname=k.constname AND "
				+ "p.tabname=k.tabname AND  p.tabschema=k.tabschema ORDER BY p.tabname, p.constname";

		super.getJdbcTemplate().query(sql, new Object[] { schName }, new PrimaryKeyRowCallbackHandler(tableCache));

		log.info("DB2:" + schName + "的主键信息采集完毕!");

	}

	private class PrimaryKeyRowCallbackHandler implements RowCallbackHandler {

		private Map<String, Table> tableCache;

		public PrimaryKeyRowCallbackHandler(Map<String, Table> tableCache)
		{
			this.tableCache = tableCache;
		}

		public void processRow(ResultSet rs) throws SQLException {

			String tableName = rs.getString(Table.TABLE_NAME);

			if (tableCache.containsKey(tableName)) {

				PrimaryKey primaryKey = new PrimaryKey();
				primaryKey.setName(rs.getString(PrimaryKey.PK_NAME).trim());
				primaryKey.setColumnName(rs.getString(Column.COLUMN_NAME).trim());
				primaryKey.addAttr(PrimaryKey.KEY_SEQ, String.valueOf(rs.getInt(PrimaryKey.KEY_SEQ)));

				Table table = tableCache.get(tableName);
				table.addPrimaryKey(primaryKey);

			}
		}

	}

	/**
	 * 该接口实现真是一般般,只能取一个表的是或不是唯一的索引
	 * 
	 * @param schName
	 * @return
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	protected List<SQLIndex> getIndexs(String schName) throws SQLException {
		log.info("开始采集DB2:" + schName + "的索引信息...");
		String sql = "SELECT i.indname INDEX_NAME, i.tabschema SCH_NAME, i.tabname TABLE_NAME, ic.colname COLUMN_NAME, "
				+ "CASE WHEN i.uniquerule= 'D' THEN 'Permits duplicates' WHEN i.uniquerule='P' THEN 'Implements primary key' "
				+ "WHEN i.uniquerule= 'U' THEN 'Unique' END NON_UNIQUE, i.iid ORDINAL_POSITION,i.density CARDINALITY, i.sequential_pages PAGES "
				+ "FROM syscat.indexes i, syscat.indexcoluse ic WHERE i.indschema=ic.indschema AND "
				+ "i.indname=ic.indname AND i.indschema=? ORDER BY i.tabname";
		List<SQLIndex> indexes = super.getJdbcTemplate().query(sql, new Object[] { schName }, new IndexRowMapper());
		log.info("DB2:" + schName + "的索引采集完毕!");
		return indexes;
	}

	private class IndexRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			SQLIndex sIndex = new SQLIndex();
			sIndex.setName(rs.getString(SQLIndex.INDEX_NAME).trim());

			sIndex.setSchName(rs.getString(Schema.SCH_NAME).trim());
			sIndex.setTableName(rs.getString(Table.TABLE_NAME).trim());
			sIndex.setColumnName(rs.getString(Column.COLUMN_NAME).trim());

			sIndex.addAttr(SQLIndex.NON_UNIQUE, rs.getString(SQLIndex.NON_UNIQUE));
			sIndex.addAttr(SQLIndex.ORDINAL_POSITION, String.valueOf(rs.getInt(SQLIndex.ORDINAL_POSITION)));
			sIndex.addAttr(SQLIndex.CARDINALITY, String.valueOf(rs.getInt(SQLIndex.CARDINALITY)));
			sIndex.addAttr(SQLIndex.PAGES, String.valueOf(rs.getInt(SQLIndex.PAGES)));

			return sIndex;
		}
	}

	protected void setViewDependency(String schName, Map<String, View> viewCache) throws SQLException {
		log.info("开始采集DB2:" + schName + "视图与表的依赖信息...");
		String sql = "SELECT dname NAME, bcreator SCH_NAME, bname TABLE_NAME, CASE WHEN btype='T' THEN 'TABLE' WHEN btype='V' "
				+ "THEN 'VIEW' END REFERENCED_TYPE  FROM sysibm.sysviewdep WHERE dcreator=? AND dtype='V'";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new ViewReferenceRowCallbackHandler(viewCache));
		log.info("DB2:" + schName + "视图与表的依赖信息采集完毕!");
	}

	private class ViewReferenceRowCallbackHandler implements RowCallbackHandler {

		private Map<String, View> viewCache;

		public ViewReferenceRowCallbackHandler(Map<String, View> viewCache)
		{
			this.viewCache = viewCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String viewName = rs.getString("NAME").trim();
			if (viewCache.containsKey(viewName)) {
				// 包含于视图缓存中
				String tabName = rs.getString(Table.TABLE_NAME).trim();
				String schName = rs.getString(Schema.SCH_NAME).trim();
				String type = rs.getString(View.REFERENCED_TYPE);
				if (NamedColumnSetType.TABLE.toString().equals(type)) {
					viewCache.get(viewName).addReferenceSchTable(schName.concat(".").concat(tabName),
							NamedColumnSetType.TABLE);
				}
				else if (NamedColumnSetType.VIEW.toString().equals(type)) {
					viewCache.get(viewName).addReferenceSchTable(schName.concat(".").concat(tabName),
							NamedColumnSetType.VIEW);
				}
				// log.info("视图:" + viewName + "依赖的表:" +
				// schName.concat(".").concat(tabName));

			}

		}

	}

}
