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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.IExtractFilter;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.db.IDBExtractService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Catalog;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.TdMacro;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Trigger;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

public abstract class AbstractDBExtractService implements IDBExtractService {

	private Log log = LogFactory.getLog(AbstractDBExtractService.class);

	private JdbcTemplate jdbcTemplate;

	private DatabaseMetaData metaData;

	/**
	 * 需要采集的SCHEMA,需采多个时,用","分隔
	 */
	private String schemas;
	
	/**
	 * 已经采集到的表，供其他方法中使用，如采集索引时验证有效性等
	 */
	private Map<String, Table> cachedTables;

	protected JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setSchemas(String schemas) {
		this.schemas = schemas;
	}

	protected String getExtractSchemas() {
		return schemas;
	}

	protected DatabaseMetaData getMetadata() {
		return metaData;
	}

	public final Catalog getCatalog() throws SQLException {
		Connection conn = null;
		try {
			conn = jdbcTemplate.getDataSource().getConnection();
			metaData = conn.getMetaData();
		}
		catch (SQLException e) {
			log.error("连接DB数据源或查询SQL出现异常!请确认DB数据源连接参数是否正确!", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "连接DB数据源或查询SQL出现异常!请确认DB数据源连接参数是否正确!");
			if (conn != null) {
				conn.close();
			}
			throw e;
		}
		log.info("正确连接数据源数据库!");

		try {
			log.info("正确获取数据库类型.");

			String catalogName = conn.getCatalog();
			Catalog catalog = new Catalog();
			catalog.setName(catalogName);
			log.info("catalog Name:" + catalogName);
			catalog.setSchemas(internalGetSchemas());

			internalAfterHook(catalog);
			return catalog;
		}
		catch (SQLException e) {
			log.error("", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, e.getMessage());
			throw e;
		}
		finally {
			// 采集数据的最后,及时关闭数据源连接
			if (conn != null) {
				conn.close();
			}
		}
	}

	private void internalAfterHook(Catalog catalog) {
		filtrate(catalog);

		afterHook(catalog);
	}

	private void filtrate(Catalog catalog) {
		IExtractFilter filter = getExtractFilter();
		if (filter != null)
			filter.filtrate(catalog);

	}

	protected void afterHook(Catalog catalog) {
		// 勾子,不做任何事,如需什么善后处理,请重写并实现之

	}

	/**
	 * 不允许继承
	 */
	private List<Schema> internalGetSchemas() throws SQLException {
		try {
			List<Schema> schemas = getSchemas();
			for (int i = 0; schemas != null && i < schemas.size(); i++) {
				Schema schema = schemas.get(i);
				String schName = schema.getName();
				String dataType = getMetadata().getDatabaseProductName();
				if(dataType.equals("PostgreSQL")){
					schName = schName.toLowerCase();
				}
				log.info("schema:" + schName);
				schema.addColumnSet(internalGetTables(schName));
				schema.addColumnSet(internalGetViews(schName));
				schema.setMacros(getMacro(schName));
				schema.setProcedures(internalGetProcedures(schName));
				schema.setTriggers(getTriggers(schName));
				schema.setSQLIndexs(internalGetIndexs(schName));
			}
			return schemas;
		}
		catch (BadSqlGrammarException e) {
			if (e.getMessage().indexOf(signOfNoPrivilege()) > -1) {
				log.warn("访问系统表的权限不足！请检查是否有访问如下系统表的权限:" + getSystemTableList().toString(), e);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "访问系统表的权限不足！请检查是否有访问如下系统表的权限:"
						+ getSystemTableList().toString());
			}
			throw e;
		}
	}

	/**
	 * 不允许重写
	 */
	private List<Procedure> internalGetProcedures(String schName) throws SQLException {
		List<Procedure> procedures = getProcedures(schName);
		Map<String, Procedure> procedureCache = new HashMap<String, Procedure>();
		for (int i = 0; i < procedures.size(); i++) {
			procedureCache.put(procedures.get(i).getName(), procedures.get(i));
		}
		if (procedures.size() > 0) {
			// 设置存储过程的内容
			setProceduresText(schName, procedureCache);
			setProcedureColumns(schName, procedureCache);
		}
		return procedures;
	}

	/**
	 * 不允许重写
	 */
	private List<NamedColumnSet> internalGetViews(String schName) throws SQLException {
		List<NamedColumnSet> views = getViews(schName);
		Map<String, View> viewCache = new HashMap<String, View>();
		for (int i = 0; i < views.size(); i++) {
			viewCache.put(views.get(i).getName(), (View) views.get(i));
		}
		if (views.size() > 0) {
			// 查询获取视图与表间的依赖
			setViewDependency(schName, viewCache);
			// 设置视图的SQL
			setViewText(schName, viewCache);
			// 视图字段
			setViewColumns(schName, viewCache);
		}
		return views;
	}

	/**
	 * 不允许重写
	 */
	private List<NamedColumnSet> internalGetTables(String schName) throws SQLException {
		// try {
		List<NamedColumnSet> tables = getTables(schName);
		Map<String, Table> tableCache = new HashMap<String, Table>();
		for (int i = 0; i < tables.size(); i++) {
			tableCache.put(tables.get(i).getName(), (Table) tables.get(i));
		}
		this.cachedTables = tableCache;
		if (tables.size() > 0) {
			setTableColumns(schName, tableCache);
			setPrimaryKeies(schName, tableCache);
			setForeignKeies(schName, tableCache);
			setPartitions(schName, tableCache);
			setConstraints(schName, tableCache);
		}
		return tables;
	}

	private List<SQLIndex> internalGetIndexs(String schName) throws SQLException {
		List<SQLIndex> indexes = getIndexs(schName);
		for (SQLIndex index : indexes) {
			String indexSchName = index.getSchName();
			if (indexSchName == null || indexSchName.equals("")) {
				index.setSchName(schName);
			}
		}

		return indexes;
	}

	/**
	 * 获取已经采集到的有效的表
	 * @return 表map
	 */
	protected Map<String, Table> getCachedTables() {
		return this.cachedTables;
	}
	
	/**
	 * 是否权限不足的标记
	 * 
	 * @return
	 */
	protected abstract String signOfNoPrivilege();

	/**
	 * 需要读取访问的系统表
	 * 
	 * @return
	 */
	protected abstract List<String> getSystemTableList();

	/**
	 * 过滤器
	 * 
	 * @return
	 */
	protected abstract IExtractFilter getExtractFilter();

	protected abstract void setProcedureColumns(String schName, Map<String, Procedure> procedureCache)
			throws SQLException;

	protected abstract void setProceduresText(String schName, Map<String, Procedure> procedureCache)
			throws SQLException;

	protected abstract List<Procedure> getProcedures(String schName) throws SQLException;

	protected abstract void setViewDependency(String schName, Map<String, View> viewCache) throws SQLException;

	protected abstract void setViewText(String schName, Map<String, View> viewCache) throws SQLException;

	protected abstract void setViewColumns(String schName, Map<String, View> viewCache) throws SQLException;

	protected abstract List<NamedColumnSet> getViews(String schName) throws SQLException;

	protected abstract void setTableColumns(String schName, Map<String, Table> tableCache) throws SQLException;

	protected abstract void setPrimaryKeies(String schName, Map<String, Table> tableCache) throws SQLException;

	protected abstract void setForeignKeies(String schName, Map<String, Table> tableCache) throws SQLException;

	protected abstract void setPartitions(String schName, Map<String, Table> tableCache) throws SQLException;

	protected abstract void setConstraints(String schName, Map<String, Table> tableCache) throws SQLException;

	protected abstract List<NamedColumnSet> getTables(String schName) throws SQLException;

	protected abstract List<TdMacro> getMacro(String schName) throws SQLException;

	protected abstract List<Trigger> getTriggers(String schName) throws SQLException;

	protected abstract List<Schema> getSchemas() throws SQLException;

	protected abstract List<SQLIndex> getIndexs(String schName) throws SQLException;

}
