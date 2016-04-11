/**  
 * 文件名：CGBAS400ExtractServiceImpl.java   

 *   
 * 版本信息：   
 * 日期：2012-8-24   
 * Copyright 足下 Corporation 2012    
 * 版权所有   
 *   
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;

/** 
 * 
 * 项目名称：metamanager 
 * 类名称：CGBAS400ExtractServiceImpl 
 * 类描述： 
 * 创建人：kaishui 
 * 创建时间：2012-8-24 上午10:01:31 
 * 
 * @version 1.0
 * 
 */
public class JiCaiAS400ExtractServiceImpl extends AbstractJiCaiDBExtractService {
	private final static List<String> systemTableList = new ArrayList<String>(7);

	/**
	 * 
	 * DB配置参数
	 * 
	 *  (一)  表级（EXTTABP）
	 *  (二)  字段级（EXTFLDP）
	 * 
	 */
	{
		if (properties != null) {
			systemTableList.add(properties.getProperty("EXTFLDP"));
			systemTableList.add(properties.getProperty("EXTTABP"));
		}
	}

	private Log log = LogFactory.getLog(JiCaiAS400ExtractServiceImpl.class);

	@Override
	protected String signOfNoPrivilege() {
		return "表或视图不存在";
	}

	@Override
	protected List<String> getSystemTableList() {
		return systemTableList;
	}

	@SuppressWarnings("unchecked")
	protected List<TableVO> getTables(String schName, int start, int limit)
			throws SQLException {
		log.info("开始采集表信息...");
		String sql = "select '"
				+ properties.getProperty("sysName")
				+ "' EXTSYS , "//--系统名称
				+ " '" + properties.getProperty("ESysName")
				+ "' EXTSYE , "//--英文缩写
				+ " 	EXTMOD , "//--系统模块
				+ " 	replace(EXTTBE,chr(127),'\\') as EXTTBE  , "//--表英文名
				+ " 	EXTTBC , "//--表中文名
				+ " 	EXTTBD , "//--描述 
				+ " 	EXTTBT , "//--表类型
				+ " 	EXTTBK , "//--是否存在主键
				+ " 	replace(EXTIDY,chr(127),'\\') as EXTIDY , "//--唯一索引
				+ " 	replace(EXTIDN,chr(127),'\\') as EXTIDN  "//--非唯一索引
				+ " 	from " + properties.getProperty("EXTTABP")
				+ " f where f.extmod = ? ";
		Object[] objs = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql += " and f.src_nme = ? ";
			objs = new Object[] { schName, SRC_NME };
		} else {
			objs = new Object[] { schName };
		}
		sql = getRownumSql(sql, start, limit);
		return super.getJdbcTemplate().query(sql, objs, new TableRowMapper());
	}

	private class TableRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			TableVO table = new TableVO();
			table.setSysName(rs.getString("EXTSYS"));
			table.seteSysName(rs.getString("EXTSYE"));
			table.setDbName(rs.getString("EXTMOD"));
			table.setTableName(rs.getString("EXTTBC"));
			table.seteTableName(rs.getString("EXTTBE"));
			table.setDesc(rs.getString("EXTTBD"));
			table.setTableType(rs.getString("EXTTBT"));
			table.setIsPk(rs.getString("EXTTBK"));
			table.setUniqueIndexName(rs.getString("EXTIDY"));
			table.setNonuniqueIndexName(rs.getString("EXTIDN"));
			return table;
		}

	}

	@SuppressWarnings("unchecked")
	protected List<ColumnVO> getFields(String schName, int start, int limit)
			throws SQLException {
		log.info("开始采集表的字段信息...");
		String sql = "select '"
				+ properties.getProperty("sysName")
				+ "' EXTSYS , "//--系统名称
				+ " '" + properties.getProperty("ESysName")
				+ "' EXTSYE , "//--英文缩写
				+ " EXTMOD , "//--系统模块
				+ " replace(EXTTBE,chr(127),'\\') as EXTTBE, "//--表英文名
				+ " EXTTBC , "//--表中文名
				+ " EXTFNB , "//--字段序号
				+ " replace(EXTFDE,chr(127),'\\') as EXTFDE, "//--字段英文名
				+ " EXTFDC , "//--字段中文名
				+ " EXTFDT , "//--字段类型
				+ " EXTFDK , "//--主键标志
				+ " EXTFNU  "//--是否允许空值
				+ " from " + properties.getProperty("EXTFLDP")
				+ " t where t.extmod = ? ";
		Object[] objs = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql += " and src_nme = ? ";
			objs = new Object[] { schName, SRC_NME };
		} else {
			objs = new Object[] { schName };
		}
		sql = getRownumSql(sql, start, limit);
		return super.getJdbcTemplate().query(sql, objs,
				new ColumnRowCallbackHandler());
	}

	private class ColumnRowCallbackHandler implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			ColumnVO field = new ColumnVO();
			field.setSysName(rs.getString("EXTSYS"));
			field.seteSysName(rs.getString("EXTSYE"));
			field.setDbName(rs.getString("EXTMOD"));
			field.setTableName(rs.getString("EXTTBC"));
			field.seteTableName(rs.getString("EXTTBE"));
			field.setFieldOrd(rs.getString("EXTFNB"));
			field.setFieldName(rs.getString("EXTFDC"));
			field.seteFieldName(rs.getString("EXTFDE"));
			field.setFieldType(rs.getString("EXTFDT"));
			field.setKeyFlag(rs.getString("EXTFDK"));
			field.setNullFlag(rs.getString("EXTFNU"));
			return field;
		}
	}

	public int getFieldSize(String schName) throws SQLException {
		String sql = "select count(1) from " + properties.getProperty("EXTFLDP")
				+ " t where t.extmod = ? ";
		Object[] objs = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql += " and src_nme = ? ";
			objs = new Object[] { schName, SRC_NME };
		} else {
			objs = new Object[] { schName };
		}
		sql = "select count(1) from (" + sql + ") c ";
		return super.getJdbcTemplate().queryForInt(sql, objs);
	}

	public int getTableSize(String schName) throws SQLException {
		String sql = "select count(1) from " + properties.getProperty("EXTTABP")
				+ " f where f.extmod = ? ";
		Object[] objs = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql += " and f.src_nme = ? ";
			objs = new Object[] { schName, SRC_NME };
		} else {
			objs = new Object[] { schName };
		}
		sql = "select count(1) from (" + sql + ") c ";
		return super.getJdbcTemplate().queryForInt(sql, objs);
	}

	public String getRownumSql(String sql, int start, int limit)
			throws SQLException {
		Connection conn = super.getJdbcTemplate().getDataSource().getConnection();
		java.sql.DatabaseMetaData meta = conn.getMetaData();
		String type = meta.getDatabaseProductName();
		String rowNumSql = "";
		if (type.matches("Oracle")) {
			rowNumSql = "SELECT * FROM (SELECT A.*, ROWNUM RN  FROM (" + sql
					+ ") A   WHERE ROWNUM <= " + limit + ") WHERE RN >= "
					+ start + "";
		} else if (type.matches("DB2/.+")) {
			rowNumSql = "select * from ( select tt.* , rownumber() over() as rowno from ("
					+ sql
					+ " )tt ) temp where temp.rowno between "
					+ start
					+ " and " + limit + " ";
		}
		if(conn!=null){
			conn.close();
		}
		return rowNumSql;
	}

	public void initHSQLDB(String schName) throws Exception {
		// TODO Auto-generated method stub
		
	}

	

}
