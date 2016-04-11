/**  
 * 文件名：CGBSQLExtractServiceImpl.java   

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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.util.JiCaiHSqlDBUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.util.HSqlDB;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.JiCaiDBMappingServiceImpl;

/** 
 * 
 * 项目名称：metamanager 
 * 类名称：CGBSQLExtractServiceImpl 
 * 类描述： 
 * 创建人：kaishui 
 * 创建时间：2012-8-24 上午10:01:31 
 * 
 * @version 1.0
 * 
 */
public class JiCaiSQLExtractServiceImpl extends AbstractJiCaiDBExtractService {
	private final static List<String> systemTableList = new ArrayList<String>(7);
	/**
	 * 用于生成临时表的表名
	 */
	private final static String tmpTable = "tmp_is_unique_"
			+ System.currentTimeMillis();
	private final static String dbType = "SqlServer";
	/**
	 * 
	 * DB配置参数
	 * 
		 DBO.SYSOBJECTS	系统对象表
		 DBO.SYSINDEXES	索引信息表
		 DBO.SYSINDEXKEYS 索引键和列信息表
		 DBO.SYSCOLUMNS	列信息表
		 DBO.SYSTYPES	数据类型表
		 SYS.EXTENDED_PROPERTIES	注释信息表
		 SYS.INDEXES	索引信息表
		 INFORMATION_SCHEMA.TABLES	表级信息表
	 * 
	 */
	{
		if (properties != null) {
			systemTableList.add(properties.getProperty("SYSOBJECTS"));
			systemTableList.add(properties.getProperty("SYSINDEXES"));
			systemTableList.add(properties.getProperty("SYSINDEXKEYS"));
			systemTableList.add(properties.getProperty("SYSCOLUMNS"));
			systemTableList.add(properties.getProperty("SYSTYPES"));
			systemTableList.add(properties.getProperty("EXTENDED_PROPERTIES"));
			systemTableList.add(properties.getProperty("INDEXES"));
			systemTableList.add(properties.getProperty("TABLES"));
			SRC_NME = properties.getProperty("SRC_NME");
		}
	}

	private Log log = LogFactory.getLog(JiCaiSQLExtractServiceImpl.class);

	@Override
	protected String signOfNoPrivilege() {
		return "表或视图不存在";
	}

	@Override
	protected List<String> getSystemTableList() {
		return systemTableList;
	}

	private String getTabSql(String SrcNme) {
		String sysobjects = properties.getProperty("SYSOBJECTS");
		String sysindexes = properties.getProperty("SYSINDEXES");
		String sysindexkeys = properties.getProperty("SYSINDEXKEYS");
		String syscolumns = properties.getProperty("SYSCOLUMNS");
		String extended_properties = properties.getProperty("EXTENDED_PROPERTIES");
		String tables = properties.getProperty("TABLES");
		String sql = "select '"
				+ properties.getProperty("sysName")
				+ "' AS sysName, "
				+ " '"
				+ properties.getProperty("ESysName")
				+ "' AS ESysName, "
				+ " t.table_schema as dbName, "//'系统模块'
				+ " d.name as eTableName , "//'表英文名'
				+ " CASE WHEN f.value is null then '' else f.value end as tableName, "//'表中文名'
				+ " '' AS des, "//'描述'
				+ " t.table_type AS table_type, "//'表类型'
				+ " CASE WHEN H.PK>0 THEN 'Y' ELSE 'N' END AS isPk, "//'是否存在主键'
				+ " w.IsUnique AS UNIQUE_index_name, "//'唯一索引'
				+ " w.NoUnique AS NONUNIQUE_index_name "//'非唯一索引'
				+ " from "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " d "
				+ " left join "
				+ extended_properties.substring(extended_properties.indexOf(".")+1)
				+ " f "
				+ " on d.id=f.major_id and f.Minor_id=0 "
				+ " join  "
				+ " ( select NAME,SUM(PK) as PK "
				+ " from ( "
				+ " select d.name, "
				+ " case when (SELECT 1 FROM "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " where xtype='PK' and "
				+ " parent_obj=a.id and name in ( "
				+ " SELECT name FROM "
				+ sysindexes.substring(sysindexes.indexOf(".")+1)
				+ " WHERE indid in( "
				+ " SELECT indid FROM "
				+ sysindexkeys.substring(sysindexkeys.indexOf(".")+1)
				+ " WHERE id = a.id AND colid=a.colid))) is null then 0 else 1 end AS PK "
				+ " from "
				+ syscolumns.substring(syscolumns.indexOf(".")+1)
				+ " a join "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " d on a.id=d.id "
				+ " where d.xtype='U' and d.name<>'dtproperties'  "
				+ " ) Q GROUP BY NAME "
				+ " ) h  "
				+ " on d.name=h.name "
				+ " left join "
				+ " ( "
				+ " SELECT 	c.name, "
				+ " (case when tmp.is_unique = '1' then tmp.indexName end )AS IsUnique, "
				+ " (case when tmp.is_unique = '0' then tmp.indexName end ) as NoUnique "
				+ " FROM "
				+ sysindexes.substring(sysindexes.indexOf(".")+1)
				+ " a LEFT JOIN "
				+ sysindexkeys.substring(sysindexkeys.indexOf(".")+1)
				+ " b ON a.id = b.id AND a.indid = b.indid LEFT JOIN "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " c ON a.id = c.id AND c.xtype = 'U' LEFT OUTER JOIN "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " e ON e.name = a.name AND e.xtype = 'UQ' LEFT OUTER JOIN "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " p ON p.name = a.name AND p.xtype = 'PK' "
				+ " left join "
				+ tmpTable
				+ " tmp on tmp.id = c.id and tmp.tableName = c.name "
				+ " WHERE c.xtype = 'U' "
				+ " and a.name is not null "
				+ " group by c.name,c.id,tmp.indexName,tmp.is_unique "
				+ " ) w on w.name=d.name  "
				+ " left join "
				+ tables.substring(tables.indexOf(".")+1)
				+ " t on t.table_name = d.name "
				+ " where d.xtype='U' and d.name<>'dtproperties'and (t.table_schema = ? or t.table_schema = ? ) ";
		if (SrcNme != null && !"".equals(SrcNme)) {
			sql += " and t.SRC_NME = ? ";
		}
		return sql;
	}

	private String getFieldSql(String SrcNme) {
		String sysobjects = properties.getProperty("SYSOBJECTS");
		String sysindexes = properties.getProperty("SYSINDEXES");
		String sysindexkeys = properties.getProperty("SYSINDEXKEYS");
		String syscolumns = properties.getProperty("SYSCOLUMNS");
		String systypes = properties.getProperty("SYSTYPES");
		String extended_properties = properties.getProperty("EXTENDED_PROPERTIES");
		String tables = properties.getProperty("TABLES");
		String sql = "SELECT '"
				+ properties.getProperty("sysName")
				+ "' AS sysName, "
				+ " '"
				+ properties.getProperty("ESysName")
				+ "' AS eSysName, "
				+ " t.table_schema as dbName, "//'系统模块'
				+ " d.name as eTableName, "//'表英文名'
				+ " CASE WHEN f.value IS NULL THEN '' ELSE f.value end as tableName, "//'表中文名'
				+ "  a.colorder as  fieldOrd, "//'字段序号'
				+ " a.name as eFieldName, "//'字段英文名'
				+ " CASE WHEN g.value IS NULL THEN '' ELSE g.value end as fieldNme, "// '字段中文名'
				+ " b.name || '(' || rtrim(cast( a.prec as char(30) )) || ','|| case when rtrim(cast(a.scale as char(30)))is null then '0' else rtrim(cast(a.scale as char(30)))end  || ')' as fieldType, "//'字段类型'
				+ " case "
				+ " when exists "
				+ " (SELECT 1 "
				+ " FROM "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " "
				+ " where xtype = 'PK' "
				+ " and parent_obj = a.id "
				+ " and name in "
				+ " (SELECT name "
				+ " FROM "
				+ sysindexes.substring(sysindexes.indexOf(".")+1)
				+ " "
				+ " WHERE indid in (SELECT indid "
				+ " FROM "
				+ sysindexkeys.substring(sysindexkeys.indexOf(".")+1)
				+ " "
				+ " WHERE id = a.id "
				+ " AND colid = a.colid))) then "
				+ " 'Y' "
				+ " else "
				+ " 'N' "
				+ " end as KEYFLAG, "//'主键'
				+ " case "
				+ " when a.isnullable = 1 then "
				+ " 'Y' "
				+ " else "
				+ " 'N' "
				+ " end as nullflag "//'是否允许空'
				+ " FROM " + syscolumns.substring(syscolumns.indexOf(".")+1) + " a "
				+ " left join " + systypes.substring(systypes.indexOf(".")+1) + " b "
				+ " on a.xusertype = b.xusertype " + " inner join "
				+ sysobjects.substring(sysobjects.indexOf(".")+1) + " d "
				+ " on a.id = d.id " + " and d.xtype = 'U' "
				+ " and d.name <> 'dtproperties' " + " left join "
				+ tables.substring(tables.indexOf(".")+1) + " t  "
				+ " on t.table_name = d.name " + " left join "
				+ extended_properties.substring(extended_properties.indexOf(".")+1) + " f "
				+ " on d.id = f.major_id " + " and f.Minor_id = 0 "
				+ " left join " + extended_properties.substring(extended_properties.indexOf(".")+1)
				+ " g " + " on a.id = g.major_id "
				+ " and a.colid = g.Minor_id " + " where (t.table_schema = ? or t.table_schema = ? ) ";
		if (SrcNme != null && !"".equals(SrcNme)) {
			sql += " and t.SRC_NME = ? ";
		}
		return sql;
	}

	/**
	 * @throws SQLException 
	  * 初始化数据，不同类型数据库中的函数不一致，无法统一SQL
	  * ，现通过代码实现SQL查询索引功能，并保存到临时表中，
	  * 当执行完成后，删除该临时表
	  * @Title: insTmpTab  
	  * @Description: TODO 
	  * @param  
	  * @return void 
	  * @throws
	 */
	private void insTmpTab(String schName) throws SQLException {
		//创建临时存放表
		String sql = "create table "
				+ tmpTable
				+ "(id varchar(128),tableName varchar(128),indexName varchar(512),is_Unique varchar(1))";
		log.info("SQL【创建" + tmpTable + "临时表...】");
		creTable(sql);
		log.info("SQL【采集TmpTabData数据...】");
		List<Map<String, String>> listMaps = getTmpTabData(schName);
		log.info("SQL【对TmpTabData数据做合并处理...】");
		listMaps = concatTmpData(listMaps);
		log.info("SQL【插入" + tmpTable + "临时表...】");
		//插入数据
		insTmpData(listMaps);
	}

	private void creTable(String sql) throws SQLException {
		try {
			super.getHsqlJdbcTemplate().executeQuery(sql);
		} catch (Exception e) {
			super.getHsqlJdbcTemplate().executeQuery("drop table " + tmpTable);
			super.getHsqlJdbcTemplate().executeQuery(sql);
		}
	}

	private void delTable() throws SQLException {
		log.info("删除" + tmpTable + "临时表...");
		String sql = " drop table " + tmpTable;
		super.getHsqlJdbcTemplate().executeQuery(sql);
	}

	private void insTmpData(List<Map<String, String>> listMaps)
			throws SQLException {
		PreparedStatement ps = null;
		Connection conn = null;
		try {
			//获取数据库连接
			conn = HSqlDB.getConnection();
			//设置提交为false，当数据处理完成后，一次性提交
			conn.setAutoCommit(false);
			for (Map<String, String> map : listMaps) {
				String sql = "insert into " + tmpTable + " values('"
						+ map.get("id") + "','" + map.get("TableName") + "','"
						+ map.get("IndexName") + "','" + map.get("is_unique")
						+ "') ";
				ps = conn.prepareStatement(sql);
				ps.execute();
				ps.close();
			}
			//再一次提交数据
			conn.commit();
		} catch (Exception e) {
			//删除临时表
			log.info("插入临时表" + tmpTable + "数据失败，错误信息：" + e);
			delTable();
		}
	}

	private List<Map<String, String>> concatTmpData(
			List<Map<String, String>> listMaps) {

		List<Map<String, String>> concatList = new ArrayList<Map<String, String>>();

		for (Map<String, String> map : listMaps) {
			Map<String, String> concatMap = new HashMap<String, String>();
			String id = map.get("id");
			String tableName = map.get("TableName");
			String indexName = map.get("IndexName");
			String isUnique = map.get("is_unique");
			boolean flag = true;
			for (Map<String, String> concatMaps : concatList) {
				if (concatMaps.get("TableName").equals(tableName)
						&& concatMaps.get("id").equals(id)
						&& concatMaps.get("is_unique").equals(isUnique)) {

					concatMaps.put("IndexName", concatMaps.get("IndexName")
							+ "|" + indexName);
					flag = false;
					break;
				}
			}
			if (flag) {
				concatMap.put("id", id);
				concatMap.put("TableName", tableName);
				concatMap.put("IndexName", indexName);
				concatMap.put("is_unique", isUnique);
				concatList.add(concatMap);
			}
		}
		//清空
		listMaps.clear();
		return concatList;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, String>> getTmpTabData(String schName)
			throws SQLException {
		String sysobjects = properties.getProperty("SYSOBJECTS");
		String sysindexes = properties.getProperty("SYSINDEXES");
		String sysindexkeys = properties.getProperty("SYSINDEXKEYS");
		String indexes = properties.getProperty("INDEXES");
		String tables = properties.getProperty("TABLES");
		String[] param = null;
		String sql = "SELECT distinct "
				+ " c.id, "
				+ " c.name as TableName, "
				+ " a.name as IndexName, "
				+ " CASE WHEN ind.is_unique IS NULL THEN 0 ELSE ind.is_unique END as is_unique "
				+ " FROM "
				+ sysindexes.substring(sysindexes.indexOf(".")+1)
				+ " a LEFT JOIN "
				+ " "
				+ sysindexkeys.substring(sysindexkeys.indexOf(".")+1)
				+ " b ON a.id = b.id AND a.indid = b.indid LEFT JOIN "
				+ " "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " c ON a.id = c.id AND c.xtype = 'U' LEFT OUTER JOIN "
				+ " "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " e ON e.name = a.name AND e.xtype = 'UQ' LEFT OUTER JOIN "
				+ " "
				+ sysobjects.substring(sysobjects.indexOf(".")+1)
				+ " p ON p.name = a.name AND p.xtype = 'PK' LEFT JOIN "
				+ " "
				+ indexes.substring(indexes.indexOf(".")+1)
				+ " ind on c.id = ind.object_id and a.name = ind.name "
				+ " left join "
				+ tables.substring(tables.indexOf(".")+1)
				+ " t on t.TABLE_NAME = c.name "
				+ " 	WHERE c.xtype = 'U' "
				+ " 	and a.name is not null and (t.table_schema = ? or t.table_schema = ? ) ";
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			param = new String[] { schName.toLowerCase(),schName.toUpperCase(), SRC_NME };
			sql += " and t.SRC_NME = ? ";
		} else {
			param = new String[] { schName.toLowerCase(),schName.toUpperCase() };
		}
		return getHsqlJdbcTemplate().queryForList(sql, param);

	}

	protected List<TableVO> getTables(String schName, int start, int limit)
			throws SQLException {
		//在采集table字典前，需对uniq数据进行初始化临时表。
		try {
			log.info("开始采集表信息...");
			String sql = "";
			String[] param = null;
			//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
			if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
				param = new String[] { schName.toLowerCase(),schName.toUpperCase(), SRC_NME };
			} else {
				param = new String[] { schName.toLowerCase(),schName.toUpperCase() };
			}
			//获取得到表级信息SQL
			sql = getTabSql(SRC_NME);
			//根据不同类型的数据，得到不同的分页查询SQL
			sql = getRownumSql(sql, start, limit);
			List<TableVO> lists = getHsqlJdbcTemplate().queryForListToTableVO(
					sql, param);
			if (lists == null) {
				delTable();
				return new ArrayList<TableVO>();
			}
			//如果采集的数据字典长度小于已定义的长度，那么及时删除临时表
			if (lists.size() < JiCaiDBMappingServiceImpl.maxCount) {
				delTable();
			}
			return lists;
		} catch (Exception e) {
			//当table数据采集完成后，需删除临时表
			delTable();
			throw new SQLException(e.getMessage());
		}
	}

	protected List<ColumnVO> getFields(String schName, int start, int limit)
			throws SQLException {
		log.info("开始采集表的字段信息...");
		String sql = "";
		String[] param = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			param = new String[] { schName.toLowerCase(),schName.toUpperCase(), SRC_NME };
		} else {
			param = new String[] { schName.toLowerCase(),schName.toUpperCase() };
		}
		//获取字段级信息表
		sql = getFieldSql(SRC_NME);
		//根据不同类型的数据，得到不同的分页查询SQL
		sql = getRownumSql(sql, start, limit);
		List<ColumnVO> lists = super.getHsqlJdbcTemplate()
				.queryForListToFieldVO(sql, param);
		if (lists == null) {
			return new ArrayList<ColumnVO>();
		}
		return lists;
	}

	public int getFieldSize(String schName) throws SQLException {
		log.info("获取表的字段信息长度...");
		String sql = "";
		String[] param = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			param = new String[] { schName.toLowerCase(),schName.toUpperCase(), SRC_NME };
		} else {
			param = new String[] { schName.toLowerCase(),schName.toUpperCase() };
		}
		sql = "select count(1) from (" + getFieldSql(SRC_NME) + ") c ";

		return super.getHsqlJdbcTemplate().queryForInt(sql, param);
	}

	public int getTableSize(String schName) throws SQLException {
		try {
			//在采集table过程中，需对uniq数据进行初始化临时表。
			insTmpTab(schName);
			log.info("获取表级信息长度...");
			String sql = "";
			String[] param = null;
			//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
			if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
				param = new String[] { schName.toLowerCase(),schName.toUpperCase(), SRC_NME };
			} else {
				param = new String[] { schName.toLowerCase(),schName.toUpperCase() };
			}
			sql = "select count(1) from (" + getTabSql(SRC_NME) + ") c ";
			return super.getHsqlJdbcTemplate().queryForInt(sql, param);
		} catch (Exception e) {
			//当table数据采集完成后，需删除临时表
			delTable();
			throw new SQLException(e.getMessage());
		}

	}

	public String getRownumSql(String sql, int start, int limit)
			throws SQLException {
		//		Connection conn = super.getJdbcTemplate().getDataSource()
		//				.getConnection();
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
		//		if (conn != null) {
		//			conn.close();
		//		}
		rowNumSql = "select limit " + start + " " + limit + " * from (" + sql
				+ ") ";
		return rowNumSql;
	}

	public void initHSQLDB(String schName) throws Exception {
		// TODO Auto-generated method stub
		JiCaiHSqlDBUtil hsql = new JiCaiHSqlDBUtil();
		hsql.initSystemTable(properties, dbType, super.getJdbcTemplate()
				.getDataSource().getConnection(), schName);
	}
}
