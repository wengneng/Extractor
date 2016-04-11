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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.JiCaiHSqlDBDao;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.IJiCaiDBExtractService;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SchemaVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SystemVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

public abstract class AbstractJiCaiDBExtractService implements
		IJiCaiDBExtractService {

	private Log log = LogFactory.getLog(AbstractJiCaiDBExtractService.class);

	/**
	 * 获取前台页面配置DB参数
	 */
	protected Properties properties = AdapterExtractorContext.getInstance()
			.getParameters();

	private JiCaiHSqlDBDao hsqlDao;
	
	protected JiCaiHSqlDBDao getHsqlJdbcTemplate(){
		if(hsqlDao == null){
			hsqlDao = new JiCaiHSqlDBDaoImpl();
			return hsqlDao;
		}else{
			return hsqlDao;
		}
	}
	
	/**
	 * 获取ODS系统代码，如果用户填写，将对SQL查询条件进行变动。
	 */
	protected String SRC_NME = null;

	private JdbcTemplate jdbcTemplate;

	private DatabaseMetaData metaData;

	protected JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	protected DatabaseMetaData getMetadata() {
		return metaData;
	}

	public final SystemVO getSysInfos(String schema, int start, int limit,boolean flag)
			throws Exception {
		Connection conn = null;
		try {
			conn = jdbcTemplate.getDataSource().getConnection();
			metaData = conn.getMetaData();
			log.info("正确连接数据源数据库!");
			return internalGetSchemas(schema,start,limit,flag);
		} catch (Exception e) {
			log.error("连接DB数据源或查询SQL出现异常!请确认DB数据源连接参数是否正确!", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"连接DB数据源或查询SQL出现异常!请确认DB数据源连接参数是否正确!");
			if (conn != null) {
				conn.close();
			}
			throw e;
		} finally {
			// 采集数据的最后,及时关闭数据源连接
			if (conn != null) {
				conn.close();
			}
		}
	}

	/**
	 * 不允许继承
	 */
	private SystemVO internalGetSchemas(String schema, int start, int limit,boolean flag)
			throws Exception {
		try {
			//获取ODS系统代码值
			SRC_NME = properties.getProperty("SRC_NME");
			SystemVO sysInfo = new SystemVO();
			List<SchemaVO> schemasVO = new ArrayList<SchemaVO>();
			//获取系统中文名称
			sysInfo.setSysName(properties.getProperty("sysName"));
			//获取系统英文名称
			sysInfo.seteSysName(properties.getProperty("ESysName"));
			//保存schemaName、表、字段数据集合
			SchemaVO schemasvo = new SchemaVO();
			List<TableVO> tables = null;
			if(flag){
				tables = getTables(schema, start, limit);
			}else{
				tables = new ArrayList<TableVO>();
			}
			log.info("表级信息长度为:" + tables.size());
			//获取字段级信息
			List<ColumnVO> fields = getFields(schema, start, limit);
			log.info("字段级信息长度:" + fields.size());
			schemasvo.setTables(tables);
			schemasvo.setFields(fields);
			schemasvo.setSchName(schema);
			schemasVO.add(schemasvo);
			sysInfo.setSchemas(schemasVO);
			return sysInfo;
		} catch (BadSqlGrammarException e) {
			if (e.getMessage().indexOf(signOfNoPrivilege()) > -1) {
				log.warn("访问系统表的权限不足！请检查是否有访问如下系统表的权限:"
						+ getSystemTableList().toString(), e);
				AdapterExtractorContext.addExtractorLog(
						ExtractorLogLevel.ERROR, "访问系统表的权限不足！请检查是否有访问如下系统表的权限:"
								+ getSystemTableList().toString());
			}
			throw e;
		}
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
	 * 获取字段级信息数据
	  * @Title: getFields  
	  * @Description: TODO 
	  * @param @param schName
	  * @param @return
	  * @param @throws SQLException 
	  * @return List<FieldVO> 
	  * @throws
	 */
 	protected abstract List<ColumnVO> getFields(String schName, int start,
			int limit) throws SQLException;

	/**
	 * 获取字段级信息数据
	  * @Title: getTables  
	  * @Description: TODO 
	  * @param @param schName
	  * @param @return
	  * @param @throws SQLException 
	  * @return List<TableVO> 
	  * @throws
	 */
	protected abstract List<TableVO> getTables(String schName, int start,
			int limit) throws SQLException;
}
