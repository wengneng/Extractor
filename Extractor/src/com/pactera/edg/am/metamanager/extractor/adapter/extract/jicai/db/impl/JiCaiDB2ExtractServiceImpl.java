/**  
 * 文件名：CGBDB2ExtractServiceImpl.java   

 *   
 * 版本信息：   
 * 日期：2012-8-24   
 * Copyright 足下 Corporation 2012    
 * 版权所有   
 *   
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.util.JiCaiHSqlDBUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.JiCaiDBMappingServiceImpl;

/**
 * 
 * 项目名称：metamanager 
 * 类名称：CGBDB2ExtractServiceImpl 
 * 类描述： 
 * 创建人：kaishui 
 * 创建时间：2012-8-24 上午10:01:31 
 * 
 * @version 1.0
 * 
 */
public class JiCaiDB2ExtractServiceImpl extends AbstractJiCaiDBExtractService {
	private final static List<String> systemTableList = new ArrayList<String>(7);

	private static String tmpTab1 = "tmp_tab1_" + System.currentTimeMillis();

	private static String tmpTab2 = "tmp_tab2_" + System.currentTimeMillis();

	private final static String dbType = "db2";
	
	/**
	 * 
	 * DB配置参数
	 * 
	 *  (一)  SYSCAT用户数据列描述表（SYSCAT. COLUMNS）
	 *  (二)  表级信息表（SYSCAT. TABLES）
	 *  (三)  索引表（SYSCAT.INDEXES） 
	 * 
	 */
	{
		if (properties != null) {
			systemTableList.add(properties.getProperty("SYSCAT.COLUMNS"));
			systemTableList.add(properties.getProperty("SYSCAT.TABLES"));
			systemTableList.add(properties.getProperty("SYSCAT.INDEXES"));
			SRC_NME = properties.getProperty("SRC_NME");
		}
	}

	private Log log = LogFactory.getLog(JiCaiDB2ExtractServiceImpl.class);

	@Override
	protected String signOfNoPrivilege() {
		return "表或视图不存在";
	}

	@Override
	protected List<String> getSystemTableList() {
		return systemTableList;
	}

	private String getFieSql(String schNme) throws SQLException {
		String Columns = properties.getProperty("SYSCAT.COLUMNS");
		String Tables = properties.getProperty("SYSCAT.TABLES");
		String sql = "";
		if (schNme != null) {
			sql = " SELECT " + " '"
					+ properties.getProperty("sysName")
					+ "' AS sysName, "
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' AS eSysName, "
					+ " T1.TABSCHEMA AS dbName, "
					+ " T1.TABNAME AS eTableName, "
					+ " T2.REMARKS AS tableName, "
					+ " T1.COLNO+1 AS fieldOrd, "
					+ " T1.COLNAME AS eFieldName, "
					+ " T1.REMARKS AS fieldNme, "
					+ " CASE WHEN  T1.TYPENAME='DECIMAL' then 'DECIMAL('||T1.LENGTH||','||T1.SCALE||')'  "
					+ " WHEN T1.TYPENAME='CHARACTER' then 'CHAR('||T1.LENGTH||')'   "
					+ " WHEN T1.TYPENAME='VARCHAR' then 'VARCHAR('||T1.LENGTH||')'  "
					+ " WHEN T1.TYPENAME='GRAPHIC' then 'GRAPHIC('||T1.LENGTH||')'   "
					+ " WHEN T1.TYPENAME='VARGRAPHIC' then 'VARGRAPHIC('||T1.LENGTH||')'  "
					+ " WHEN T1.TYPENAME='CLOB' then 'CLOB('||T1.LENGTH||')'     "
					+ " WHEN T1.TYPENAME='BLOB' then 'BLOB('||T1.LENGTH||')'   "
					+ " WHEN T1.TYPENAME='LONG VARCHAR' THEN 'LONG VARCHAR('||T1.LENGTH||')'   "
					+ " ELSE T1.TYPENAME END AS fieldType, "
					+ " CASE WHEN T1.KEYSEQ >0 Then 'Y'  "
					+ "         ELSE 'N' END AS KEYFLAG,  "
					+ "        T1.NULLS AS nullflag  "
					+ " FROM "
					+ Columns.substring(Columns.indexOf(".")+1)
					+ " T1 "
					+ " LEFT JOIN  "
					+ " (SELECT TABSCHEMA,TABNAME,REMARKS,SRC_NME FROM "
					+ Tables.substring(Tables.indexOf(".")+1)
					+ " WHERE TYPE='T' AND TABSCHEMA = ? )T2  "
					+ " ON T1.TABSCHEMA =  T2.TABSCHEMA AND T1.TABNAME=T2.TABNAME and T1.Src_Nme = T2.Src_Nme  "
					+ " WHERE  "
					+ " T1.TABSCHEMA = ? AND T1.Src_Nme =? "
					+ " AND T1.TABSCHEMA IS NOT NULL and T1.TABNAME IS NOT NULL ";
		} else {
			sql = " SELECT " + " '"
					+ properties.getProperty("sysName")
					+ "' AS sysName, "
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' AS eSysName, "
					+ " T1.TABSCHEMA AS dbName, "
					+ " T1.TABNAME AS eTableName, "
					+ " T2.REMARKS AS tableName, "
					+ " T1.COLNO+1 AS fieldOrd, "
					+ " T1.COLNAME AS eFieldName, "
					+ " T1.REMARKS AS fieldNme, "
					+ " CASE WHEN  T1.TYPENAME='DECIMAL' then 'DECIMAL('||T1.LENGTH||','||T1.SCALE||')'  "
					+ " WHEN T1.TYPENAME='CHARACTER' then 'CHAR('||T1.LENGTH||')'   "
					+ " WHEN T1.TYPENAME='VARCHAR' then 'VARCHAR('||T1.LENGTH||')'  "
					+ " WHEN T1.TYPENAME='GRAPHIC' then 'GRAPHIC('||T1.LENGTH||')'   "
					+ " WHEN T1.TYPENAME='VARGRAPHIC' then 'VARGRAPHIC('||T1.LENGTH||')'  "
					+ " WHEN T1.TYPENAME='CLOB' then 'CLOB('||T1.LENGTH||')'     "
					+ " WHEN T1.TYPENAME='BLOB' then 'BLOB('||T1.LENGTH||')'   "
					+ " WHEN T1.TYPENAME='LONG VARCHAR' THEN 'LONG VARCHAR('||T1.LENGTH||')'   "
					+ " ELSE T1.TYPENAME END AS fieldType, "
					+ " CASE WHEN T1.KEYSEQ >0 Then 'Y'  "
					+ "         ELSE 'N' END AS KEYFLAG,  "
					+ "        T1.NULLS AS nullflag  "
					+ " FROM "
					+ Columns.substring(Columns.indexOf(".")+1)
					+ " T1 "
					+ " LEFT JOIN  "
					+ " (SELECT TABSCHEMA,TABNAME,REMARKS FROM "
					+ Tables.substring(Tables.indexOf(".")+1)
					+ " WHERE TYPE='T' AND TABSCHEMA = ? )T2  "
					+ " ON T1.TABSCHEMA =  T2.TABSCHEMA AND T1.TABNAME=T2.TABNAME  "
					+ " WHERE  "
					+ " T1.TABSCHEMA = ? "
					+ " AND T1.TABSCHEMA IS NOT NULL and T1.TABNAME IS NOT NULL ";
		}
		return sql;
	}

	private String getTabSql(String SrcNme, boolean flag, int start, int limit)
			throws SQLException {
		String Columns = properties.getProperty("SYSCAT.COLUMNS");
		String Tables = properties.getProperty("SYSCAT.TABLES");
		String sql = "";
		if (SrcNme != null) {
			if (flag) {
				sql += "select count(1) from ( ";
			}
			String sql1 = " SELECT  " + " '"
					+ properties.getProperty("sysName")
					+ "' AS sysName, "
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' AS ESysName, "
					+ " T1.TABSCHEMA AS dbName, "
					+ " T1.TABNAME AS eTableName, "
					+ " T1.REMARKS AS tableName, "
					+ " '' AS des, "
					+ " '' AS table_type, "
					+ " CASE WHEN T3.KEYSEQ IS NOT NULL THEN 'Y' ELSE 'N' END AS isPk, "
					+ " '' AS EN_IS_IMP, "
					+ " CASE WHEN (T4.colnames is null or  T4.colnames='') then '' else  '('||T4.colnames||')' end AS UNIQUE_index_name, "
					+ " case when (T4.colnames1 is null or T4.colnames1='') then '' else '('||T4.colnames1||')'  end AS NONUNIQUE_index_name "
					+ " FROM "
					+ Tables.substring(Tables.indexOf(".")+1)
					+ " t1 "
					+ " LEFT JOIN (select distinct TABSCHEMA,TABNAME,REMARKS,KEYSEQ,Src_Nme from  "
					+ Columns.substring(Columns.indexOf(".")+1)
					+ " where KEYSEQ IS NOT NULL and keyseq=1) T3 "
					+ " ON T1.TABSCHEMA = T3.TABSCHEMA  AND  T1.TABNAME = T3.TABNAME and t1.Src_Nme = t3.Src_Nme "
					+ " LEFT JOIN  "
					+ " (select tabschema,tabname ,uniquerule,colnames,colnames1 "
					+ " from "+tmpTab2+" t2  "
					+ " where t2.child = (select max(child)  "
					+ " from "+tmpTab1+" index_table "
					+ " where  t2.tabschema = index_table.tabschema and  t2.tabname = index_table.tabname  "
					+ " ) "
					+ " ) T4 ON  T1.TABSCHEMA = T4.tabschema  AND  T1.TABNAME = T4.tabname "
					+ " WHERE  T1.TABSCHEMA = ? and t1.Src_Nme = ?";
			if (flag) {
				sql += sql1 + ") c";
			} else {
				sql += getRownumSql(sql1, start, limit);
			}
		} else {
			if (flag) {
				sql += " select count(1) from ( ";
			} else {

			}
			String sql1 = " SELECT  " + " '"
					+ properties.getProperty("sysName")
					+ "' AS sysName, "
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' AS ESysName, "
					+ " T1.TABSCHEMA AS dbName, "
					+ " T1.TABNAME AS eTableName, "
					+ " T1.REMARKS AS tableName, "
					+ " '' AS des, "
					+ " '' AS table_type, "
					+ " CASE WHEN T3.KEYSEQ IS NOT NULL THEN 'Y' ELSE 'N' END AS isPk, "
					+ " '' AS EN_IS_IMP, "
					+ " CASE WHEN (T4.colnames is null or  T4.colnames='') then '' else  '('||T4.colnames||')' end AS UNIQUE_index_name, "
					+ " case when (T4.colnames1 is null or T4.colnames1='') then '' else '('||T4.colnames1||')'  end AS NONUNIQUE_index_name "
					+ " FROM "
					+ Tables.substring(Tables.indexOf(".")+1)
					+ " t1 "
					+ " LEFT JOIN (select distinct TABSCHEMA,TABNAME,REMARKS,KEYSEQ from  "
					+ Columns.substring(Columns.indexOf(".")+1)
					+ " where KEYSEQ IS NOT NULL and keyseq=1) T3 "
					+ " ON T1.TABSCHEMA = T3.TABSCHEMA  AND  T1.TABNAME = T3.TABNAME "
					+ " LEFT JOIN  "
					+ " (select tabschema, tabname ,uniquerule,colnames,colnames1 "
					+ " from "+tmpTab2+" t2  "
					+ " where t2.child = (select max(child)  "
					+ " from "+tmpTab1+" index_table "
					+ " where t2.tabschema = index_table.tabschema and  t2.tabname = index_table.tabname  "
					+ " ) "
					+ " ) T4 ON  T1.TABSCHEMA = T4.tabschema  AND  T1.TABNAME = T4.tabname "
					+ " WHERE  T1.TABSCHEMA = ?  ";
			if (flag) {
				sql += sql1 + ") c ";
			} else {
				sql += getRownumSql(sql1, start, limit);
			}
		}
		return sql;
	}

	@SuppressWarnings("unchecked")
	protected List<TableVO> getTables(String schName, int start, int limit)
			throws SQLException {
		try {
			log.info("开始采集表信息...");
			String sql = "";
			String [] param = null;
			//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
			if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
				sql = getTabSql(SRC_NME, false, start, limit);
				param = new String[] { schName, SRC_NME };
			} else {
				sql = getTabSql(null, false, start, limit);
				param = new String[] { schName };
			}
			List<TableVO> lists = getHsqlJdbcTemplate().queryForListToTableVO(sql, param);
			if (lists == null) {
				executeSql("drop table " + tmpTab1,null);
				executeSql("drop table " + tmpTab2,null);
				return new ArrayList<TableVO>();
			}
			//如果采集的数据字典长度小于已定义的长度，那么及时删除临时表
			if (lists.size() < JiCaiDBMappingServiceImpl.maxCount) {
				executeSql("drop table " + tmpTab1,null);
				executeSql("drop table " + tmpTab2,null);
			}
			return lists;
		} catch (Exception e) {
			executeSql("drop table " + tmpTab1,null);
			executeSql("drop table " + tmpTab2,null);
			// TODO: handle exception
			throw new SQLException(e.getMessage());
		}
	}

//	private class TableRowMapper implements RowMapper {
//
//		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
//			TableVO table = new TableVO();
//			table.setSysName(rs.getString("SYS_NAME"));
//			table.seteSysName(rs.getString("EN_NAME"));
//			table.setDbName(rs.getString("EN_TAB_SCHEMA"));
//			table.setTableName(rs.getString("CN_TAB_NAME"));
//			table.seteTableName(rs.getString("EN_TAB_NAME"));
//			table.setDesc(rs.getString("CN_TAB_DES"));
//			table.setTableType(rs.getString("EN_TAB_TYPE"));
//			table.setIsPk(rs.getString("EN_IS_KEY"));
//			table.setUniqueIndexName(rs.getString("EN_UNIQ_KEY"));
//			table.setNonuniqueIndexName(rs.getString("EN_UNUNIQ_KEY"));
//			return table;
//		}

//	}

	@SuppressWarnings("unchecked")
	protected List<ColumnVO> getFields(String schName, int start, int limit)
			throws SQLException {
		log.info("开始采集表的字段信息...");
		String sql = "";
		String [] param = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql = getFieSql(SRC_NME);
			param = new String[] { schName, schName, SRC_NME };
		} else {
			sql = getFieSql(null);
			param = new String[] { schName, schName };
		}
		sql = getRownumSql(sql, start, limit);
		List<ColumnVO> lists = super.getHsqlJdbcTemplate().queryForListToFieldVO(sql, param);
		return lists;
	}

// 	private class ColumnRowCallbackHandler implements RowMapper {
//
//		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
//			FieldVO field = new FieldVO();
//			field.setSysName(rs.getString("SYS_NAME_CN"));
//			field.seteSysName(rs.getString("SYS_NAME_EN"));
//			field.setDbName(rs.getString("SCHEMA_NAME_EN"));
//			field.setTableName(rs.getString("TABLE_NAME_CN"));
//			field.seteTableName(rs.getString("TABLE_NAME_EN"));
//			field.setFieldOrd(rs.getString("COLUMN_NUM"));
//			field.setFieldName(rs.getString("COLUMN_NAME_CN"));
//			field.seteFieldName(rs.getString("COLUMN_NAME_EN"));
//			field.setKeyFlag(rs.getString("IS_KEY"));
//			field.setNullFlag(rs.getString("IS_NULL"));
//			field.setFieldType(rs.getString("COLUMN_TYPE"));
//			return field;
//		}
//	}

	public int getFieldSize(String schName) throws SQLException {
		String sql = "";
		String [] param = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql = getFieSql(SRC_NME);
			sql = "select count(1) from (" + sql + ") c";
			param = new String[]{ schName, schName, SRC_NME };
		} else {
			sql = getFieSql(null);
			sql = "select count(1) from (" + sql + ") c";
			param = new String[]{ schName, schName };
		}
		return super.getHsqlJdbcTemplate().queryForInt(sql,param);
	}

	public int getTableSize(String schName) throws SQLException {
		try {
			initTmpTab(schName);
			String sql = "";
			String [] param = null;
			//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
			if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
				sql = getTabSql(SRC_NME, true, 0, 0);
				param = new String[]{ schName, SRC_NME };
			} else {
				sql = getTabSql(null, true, 0, 0);
				param = new String[]{ schName };
			}
			return super.getHsqlJdbcTemplate().queryForInt(sql,param);
		} catch (SQLException e) {
			executeSql("drop table " + tmpTab1,null);
			executeSql("drop table " + tmpTab2,null);
			throw new SQLException(e.getMessage());
		}
	}

	public String getRownumSql(String sql, int start, int limit)
			throws SQLException {
//		Connection conn = super.getJdbcTemplate().getDataSource().getConnection();
//		java.sql.DatabaseMetaData meta = conn.getMetaData();
//		String type = meta.getDatabaseProductName();
		String rowNumSql = "";
//		if (type.matches("Oracle")) {
//			rowNumSql = "SELECT * FROM (SELECT A.*, ROWNUM RN  FROM (" + sql
//					+ ") A   WHERE ROWNUM <= " + limit + ") WHERE RN >= "
//					+ start + "";
//		} else if (type.matches("DB2/.+")) {
//			rowNumSql = "select * from ( select tt.* , rownumber() over() as rowno from ("
//					+ sql
//					+ " )tt ) temp where temp.rowno between "
//					+ start
//					+ " and " + limit + " ";
//		}
//		if(conn!=null){
//			conn.close();
//		}
		rowNumSql = "select limit "+start+" "+limit+" * from ("+sql+") ";
		return rowNumSql;
	}
	
	private void initTmpTab(String schName) throws SQLException{
		log.info("【开始】初始化【"+tmpTab1+"】"+"【"+tmpTab2+"】临时表！");
		String sql = "create table " + tmpTab1 + "(" + "parent integer,"
				+ "child integer," + "tabschema varchar(128),"
				+ "tabname varchar(128)," + "uniquerule varchar(32),"
				+ "colnames varchar(512)," + "colnames1 varchar(512))";
		executeSql(sql,tmpTab1);
		sql = "create table " + tmpTab2 + "(" + "parent integer,"
				+ "child integer," + "tabschema varchar(128),"
				+ "tabname varchar(128)," + "uniquerule varchar(32),"
				+ "colnames varchar(512)," + "colnames1 varchar(512),ord integer)";
		executeSql(sql,tmpTab2);
		getTmpTabData("tab1",schName);
		getTmpTabData("tab2",schName);
		log.info("【结束】初始化【"+tmpTab1+"】"+"【"+tmpTab2+"】临时表！");
		log.info("【开始】递归查询并插入【"+tmpTab1+"】"+"【"+tmpTab2+"】 临时表【子节点数据】");
		childIndexTablesT2(0);
		log.info("【结束】递归查询并插入【"+tmpTab1+"】"+"【"+tmpTab2+"】 临时表【子节点数据】");
	}
	
	private void getTmpTabData(String tab,String schName) throws SQLException {
		String sql = "";
		String whereSql = "";
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			whereSql += " and SRC_NME = '"+SRC_NME+"' ";
		}
		if (tab.equals("tab1")) {
			//sql = "insert into " + tmpTab1;
			sql = " select row_number() over (partition by tabschema,tabname order by tabschema,tabname) as parent, "
					+ " row_number() over (partition by tabschema,tabname order by tabschema,tabname)+1 as child,  "
					+ " tabschema,tabname,uniquerule,  "
					+ " case when (uniquerule ='U' or uniquerule ='P') then cast(colnames as varchar(512)) else cast('' as varchar(512)) end as colnames, "
					+ " case when uniquerule ='D' then cast(colnames as varchar(512)) else cast('' as varchar(512)) end as colnames1 "
					+ " from "+properties.getProperty("SYSCAT.INDEXES")+" "
					+ " where  uniquerule in ('U', 'P','D') and tabschema = '"+schName+"' " + whereSql;

		} else if (tab.equals("tab2")) {
			//sql = "insert into " + tmpTab2;
			sql = " select * from (select row_number() over (partition by tabschema,tabname order by tabschema,tabname) as parent, "
					+ " row_number() over (partition by tabschema,tabname order by tabschema,tabname)+1 as child,  "
					+ " tabschema,tabname,uniquerule,  "
					+ " case when (uniquerule ='U' or uniquerule ='P') then cast(colnames as varchar(512)) else cast('' as varchar(512)) end as colnames, "
					+ " case when uniquerule ='D' then cast(colnames as varchar(512)) else cast('' as varchar(512)) end as colnames1 , "
					+ 0
					+ " from "+properties.getProperty("SYSCAT.INDEXES")+" "
					+ " where  uniquerule in ('U', 'P','D') and tabschema =  '"+schName+"' "+whereSql+" ) T2 where T2.PARENT=1";
		}
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = null;
		try {
			con = super.getJdbcTemplate().getDataSource().getConnection();
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			 List<Map<String,Object>> data = new ArrayList<Map<String,Object>>();
			while(rs.next()){
				Map<String, Object> dataMap = new HashMap<String, Object>();
				int count = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= count; i++) {
					String colName = rs.getMetaData().getColumnName(i);
					Object obj = rs.getObject(colName);
					dataMap.put(colName, obj);
				}
				data.add(dataMap);
			}
			insTmpTabData(data, tab);
		} catch (SQLException e) {
			throw e;
		}finally{
			if(rs!=null)
				rs.close();
			if(ps!=null)
				ps.close();
			if(con!=null)
				con.close();
		}
		
	}
	
	public void insTmpTabData(List<Map<String, Object>> data,String tab)throws SQLException{
		String sql = "";
		for (Map<String, Object> map : data) {
			if (tab.equals("tab1")) {
				sql = "insert into " + tmpTab1;
				sql += " VALUES ("+map.get("PARENT")+","+map.get("CHILD")+",'"+map.get("TABSCHEMA").toString().trim()+"','"+map.get("TABNAME")+"','"+map.get("UNIQUERULE")+"','"+map.get("COLNAMES")+"','"+map.get("COLNAMES1")+"')";
			} else if (tab.equals("tab2")) {
				sql = "insert into " + tmpTab2;
				sql += " VALUES ("+map.get("PARENT")+","+map.get("CHILD")+",'"+map.get("TABSCHEMA").toString().trim()+"','"+map.get("TABNAME")+"','"+map.get("UNIQUERULE")+"','"+map.get("COLNAMES")+"','"+map.get("COLNAMES1")+"',0)";
			}
			super.getHsqlJdbcTemplate().executeQuery(sql);
		}
	}
	
	private void childIndexTablesT2(int ord) throws SQLException{
		AppendIndexTablesT2(ord);
		ord++;
		int updCount = getIndexTablesT2Size(ord);
		if(updCount>0){
			childIndexTablesT2(ord);
		}
	}
	
	private void executeSql(String sql,String tableName) throws SQLException {
		try {
			super.getHsqlJdbcTemplate().executeQuery(sql);
		} catch (Exception e) {
			if(tableName!=null){
				super.getHsqlJdbcTemplate().executeQuery("drop table "+tableName);
				super.getHsqlJdbcTemplate().executeQuery(sql);
			}else{
				throw new SQLException(e.getMessage());
			}
		}
	}


	private void AppendIndexTablesT2(int ord)
			throws SQLException {
		String sql = "insert into " + tmpTab2 + " ";
		sql += "select t2.child, t2.child+1,t1.tabschema,t1.tabname,t1.uniquerule, "
				+ " case when rtrim(t1.colnames)='' then t2.colnames else t2.colnames||';'||rtrim(t1.colnames) end as colnames, "
				+ " case when rtrim(t1.colnames1)='' then t2.colnames1 else t2.colnames1||';'||rtrim(t1.colnames1) end as colnames1 ,"
				+ (ord+1)
				+ " from "
				+ tmpTab2
				+ " t2, "
				+ tmpTab1
				+ " t1 "
				+ " where t2.child=t1.parent and t2.tabschema = t1.tabschema and t2.tabname = t1.tabname "
				+ " and t2.ord = "+ord;
		super.getHsqlJdbcTemplate().executeQuery(sql);
	}

	private int getIndexTablesT2Size(int ord) throws SQLException {
		String sql = "select count(1) from " + tmpTab2 +" a where a.ord = "+ord;
		return super.getHsqlJdbcTemplate().queryForInt(sql);
	}

	public void initHSQLDB(String schName) throws  Exception {
		// TODO Auto-generated method stub
		JiCaiHSqlDBUtil hsql = new JiCaiHSqlDBUtil();
		hsql.initSystemTable(properties, dbType, super.getJdbcTemplate().getDataSource().getConnection(), schName);

	}
	
	
}
