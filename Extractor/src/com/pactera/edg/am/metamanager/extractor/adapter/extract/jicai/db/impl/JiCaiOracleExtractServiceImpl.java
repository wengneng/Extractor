/**  
 * 文件名：CGBOracleExtractServiceImpl.java   

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
import java.util.Iterator;
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
 * 类名称：CGBOracleExtractServiceImpl 
 * 类描述： 
 * 创建人：kaishui 
 * 创建时间：2012-8-24 上午10:01:31 
 * 
 * @version 1.0
 * 
 */
public class JiCaiOracleExtractServiceImpl extends AbstractJiCaiDBExtractService {
	private final static List<String> systemTableList = new ArrayList<String>(7);
	/**
	 * 用于生成临时表的表名
	 */
	private final static String tmpTable = "tmp_uniqueness_"
			+ System.currentTimeMillis();
	
	private final static String dbType = "Oracle";
	/**
	 * 
	 * DB配置参数
	 * 
	 * （一） 列信息表（DBA_TAB_COLS） 
	 * （二） 表约束字段视图（DBA_CONS_COLUMNS） 
	 * （三）约束信息表（DBA_CONSTRAINTS） 
	 * （四） 表注释（DBA_TAB_COMMENTS） 
	 * （五） 列注释（DBA_COL_COMMENTS） 
	 * （六） 索引列（DBA_IND_COLUMNS） 
	 * （七） 索引（DBA_INDEXES）
	 * 
	 */
	{
		if (properties != null) {
			systemTableList.add(properties.getProperty("DBA_TAB_COLS"));
			systemTableList.add(properties.getProperty("DBA_CONS_COLUMNS"));
			systemTableList.add(properties.getProperty("DBA_CONSTRAINTS"));
			systemTableList.add(properties.getProperty("DBA_TAB_COMMENTS"));
			systemTableList.add(properties.getProperty("DBA_COL_COMMENTS"));
			systemTableList.add(properties.getProperty("DBA_IND_COLUMNS"));
			systemTableList.add(properties.getProperty("DBA_INDEXES"));
			SRC_NME = properties.getProperty("SRC_NME");
		}
	}

	private Log log = LogFactory.getLog(JiCaiOracleExtractServiceImpl.class);

	@Override
	protected String signOfNoPrivilege() {
		return "表或视图不存在";
	}

	@Override
	protected List<String> getSystemTableList() {
		return systemTableList;
	}

	private String getOraSql(String type, String SrcNme) {
		String sql = "";
		String dbaTabCols = properties.getProperty("DBA_TAB_COLS");
		String dbaConsColumns = properties.getProperty("DBA_CONS_COLUMNS");
		String dbaConstraints = properties.getProperty("DBA_CONSTRAINTS");
		String dbaTabComments = properties.getProperty("DBA_TAB_COMMENTS");
		String dbaColComments = properties.getProperty("DBA_COL_COMMENTS");
		if (type != null && type.equals("table")) {
			sql = "select '"
					+ properties.getProperty("sysName")
					+ "' as sysName, " //系统名称
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' as  ESysName,"//英文缩写
					+ " c.owner as  dbName,"//数据库名
					+ " c.table_name eTableName, "//表英文名
					+ " c.comments tableName, " //表中文名
					+ " '' as des,"//描述
					+ " c.table_type , "//表类型
					+ " CASE WHEN h.PK IS NOT NULL THEN 'Y' ELSE 'N' END as isPk , "//是否存在主键
					+ " UNIQUE_index_name , "//唯一索引
					+ " NONUNIQUE_index_name  "//非唯一索引
					+ " from "
					+ dbaTabComments.substring(dbaTabComments.indexOf(".")+1)
					+ " c "
					+ " left join "
					+ tmpTable
					+ " f "
					+ " on f.table_name = c.table_name "
					+ " and f.table_owner = c.owner and f.SRC_NME = c.SRC_NME "
					+ " left join (SELECT uc.owner, "
					+ " UC.TABLE_NAME, "
					+ " SUM(CASE "
					+ "       WHEN UCC.COLUMN_NAME IS NOT NULL THEN "
					+ "        1 "
					+ "       ELSE "
					+ "        0 "
					+ "     END) AS PK "
					+ " FROM "
					+ dbaConstraints.substring(dbaConstraints.indexOf(".")+1)
					+ " UC, "
					+ dbaConsColumns.substring(dbaConsColumns.indexOf(".")+1)
					+ " UCC "
					+ " WHERE UC.CONSTRAINT_NAME = UCC.CONSTRAINT_NAME "
					+ " AND CONSTRAINT_TYPE = 'P' and UC.SRC_NME = UCC.SRC_NME "
					+ " and uc.owner = ucc.owner "
					+ " GROUP BY uc.owner, UC.TABLE_NAME) h "
					+ " on h.table_name = c.table_name "
					+ " and h.owner = c.owner "
					+ " where c.table_name not like '%==$0' and c.owner = ? and c.SRC_NME = ? ";
		} else if (type != null && type.equals("column")) {
			sql = " select distinct '"
					+ properties.getProperty("sysName")
					+ "' sysName, "//系统名称
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' eSysName, "//英文缩写
					+ " a.owner dbName, "//系统模块
					+ " a.table_name eTableName, "//表英文名
					+ " d.COMMENTS tableName, "//表中文名
					+ " a.column_id fieldOrd, "//字段序号
					+ " a.column_name eFieldName, "//字段英文名
					+ " c.COMMENTS fieldNme, "//字段中文名
					+ "  case "
					+ " when a.data_type = 'DATE' or a.data_type = 'UROWID'or a.data_type = 'BLOB'   "
					+ " or a.data_type = 'CLOB' or a.data_type = 'NCLOB' or a.data_type = 'BFILE' "
					+ " or a.data_type = 'ROWID' or a.DATA_TYPE = 'TIMESTAMP(6)' or a.DATA_TYPE = 'LONG' "
					+ " or a.DATA_TYPE = 'REAL'  "
					+ " then a.DATA_TYPE "
					+ " when a.DATA_TYPE = 'NUMBER' or a.DATA_TYPE = 'FLOAT' then a.data_type  || case  when a.DATA_PRECISION = 0 or a.DATA_PRECISION is null then "
					+ " '' "
					+ " else "
					+ " '(' || a.DATA_PRECISION "
					+ " end  "
					+ " || case "
					+ " when a.data_scale is not null and a.data_scale > 0 then "
					+ " ',' || a.data_scale "
					+ " else "
					+ " '' "
					+ " end  "
					+ " || case "
					+ " when a.DATA_PRECISION = 0 or a.DATA_PRECISION is null then "
					+ " '' "
					+ " else "
					+ " ')' "
					+ " end "
					+ " else "
					+ " a.data_type || '(' || a.data_LENGTH || case "
					+ " when a.data_scale is not null and a.data_scale > 0 then "
					+ " ',' || a.data_scale "
					+ " else "
					+ " '' "
					+ " end || ')' "
					+ " end fieldType, "
       				+ " case "
					+ " when b.table_name is not null then "
					+ " 'Y' "
					+ " else "
					+ " 'N' "
					+ " end KEYFLAG, "//是否主键
					+ " a.nullable nullflag "//是否为NULL
					+ " FROM "
					+ dbaTabCols.substring(dbaTabCols.indexOf(".")+1)
					+ " a "
					+ " left join (SELECT UC.TABLE_NAME, UCC.COLUMN_NAME,uc.SRC_NME  "
					+ " FROM "
					+ dbaConstraints.substring(dbaConstraints.indexOf(".")+1)
					+ " UC, "
					+ dbaConsColumns.substring(dbaConsColumns.indexOf(".")+1)
					+ " UCC "
					+ " WHERE UC.CONSTRAINT_NAME = UCC.CONSTRAINT_NAME "
					+ " AND CONSTRAINT_TYPE = 'P' and uc.SRC_NME = ucc.SRC_NME) b "
					+ " on a.table_name = b.table_name "
					+ " and a.column_name = b.column_name and a.SRC_NME = b.SRC_NME"
					+ " LEFT JOIN "
					+ dbaColComments.substring(dbaColComments.indexOf(".")+1)
					+ " c "
					+ " ON a.table_name = c.TABLE_NAME "
					+ " and a.OWNER = C.OWNER and a.SRC_NME = c.SRC_NME "
					+ " AND a.column_name = c.COLUMN_NAME "
					+ " LEFT JOIN "
					+ dbaTabComments.substring(dbaTabComments.indexOf(".")+1)
					+ " d "
					+ " ON a.table_name = d.TABLE_NAME and a.SRC_NME = d.SRC_NME "
					+ " and a.OWNER = d.OWNER "
					+ " where a.table_name not like '%==$0' and  a.owner = ? and a.SRC_NME = ? ";
		}
		return sql;
	}

	private String getOraSql(String type) {
		String dbaTabCols = properties.getProperty("DBA_TAB_COLS");
		String dbaConsColumns = properties.getProperty("DBA_CONS_COLUMNS");
		String dbaConstraints = properties.getProperty("DBA_CONSTRAINTS");
		String dbaTabComments = properties.getProperty("DBA_TAB_COMMENTS");
		String dbaColComments = properties.getProperty("DBA_COL_COMMENTS");
		String sql = "";
		if (type != null && type.equals("table")) {
			sql = "select '"
					+ properties.getProperty("sysName")
					+ "' as sysName, " //系统名称
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' as  ESysName,"//英文缩写
					+ " c.owner as  dbName,"//数据库名
					+ " c.table_name eTableName, "//表英文名
					+ " c.comments tableName, " //表中文名
					+ " '' as des,"//描述
					+ " c.table_type , "//表类型
					+ " CASE WHEN h.PK IS NOT NULL THEN 'Y' ELSE 'N' END as isPk , "//是否存在主键
					+ " UNIQUE_index_name , "//唯一索引
					+ " NONUNIQUE_index_name  from "//非唯一索引
					+ dbaTabComments.substring(dbaTabComments.indexOf(".")+1)
					+ " c " + " left join " + tmpTable + " f "
					+ " on f.table_name = c.table_name "
					+ " and f.table_owner = c.owner "
					+ " left join (SELECT uc.owner, " + " UC.TABLE_NAME, "
					+ " SUM(CASE "
					+ "       WHEN UCC.COLUMN_NAME IS NOT NULL THEN "
					+ "        1 " + "       ELSE " + "        0 "
					+ "     END) AS PK " + " FROM "
					+ dbaConstraints.substring(dbaConstraints.indexOf(".")+1)
					+ " UC, "
					+ dbaConsColumns.substring(dbaConsColumns.indexOf(".")+1)
					+ " UCC "
					+ " WHERE UC.CONSTRAINT_NAME = UCC.CONSTRAINT_NAME "
					+ " AND CONSTRAINT_TYPE = 'P' "
					+ " and uc.owner = ucc.owner "
					+ " GROUP BY uc.owner, UC.TABLE_NAME) h "
					+ " on h.table_name = c.table_name "
					+ " and h.owner = c.owner " + " where c.table_name not like '%==$0' and c.owner = ? ";
		} else if (type != null && type.equals("column")) {
			sql = " select distinct '"
					+ properties.getProperty("sysName")
					+ "' sysName, "//系统名称
					+ " '"
					+ properties.getProperty("ESysName")
					+ "' eSysName, "//英文缩写
					+ " a.owner dbName, "//系统模块
					+ " a.table_name eTableName, "//表英文名
					+ " d.COMMENTS tableName, "//表中文名
					+ " a.column_id fieldOrd, "//字段序号
					+ " a.column_name eFieldName, "//字段英文名
					+ " c.COMMENTS fieldNme, "//字段中文名
					+ "  case "
					+ " when a.data_type = 'DATE' or a.data_type = 'UROWID'or a.data_type = 'BLOB'   "
					+ " or a.data_type = 'CLOB' or a.data_type = 'NCLOB' or a.data_type = 'BFILE' "
					+ " or a.data_type = 'ROWID' or a.DATA_TYPE = 'TIMESTAMP(6)' or a.DATA_TYPE = 'LONG' "
					+ " or a.DATA_TYPE = 'REAL'  "
					+ " then a.DATA_TYPE "
					+ " when a.DATA_TYPE = 'NUMBER' or a.DATA_TYPE = 'FLOAT' then a.data_type  || case  when a.DATA_PRECISION = 0 or a.DATA_PRECISION is null then "
					+ " '' "
					+ " else "
					+ " '(' || a.DATA_PRECISION "
					+ " end  "
					+ " || case "
					+ " when a.data_scale is not null and a.data_scale > 0 then "
					+ " ',' || a.data_scale "
					+ " else "
					+ " '' "
					+ " end  "
					+ " || case "
					+ " when a.DATA_PRECISION = 0 or a.DATA_PRECISION is null then "
					+ " '' "
					+ " else "
					+ " ')' "
					+ " end "
					+ " else "
					+ " a.data_type || '(' || a.data_LENGTH || case "
					+ " when a.data_scale is not null and a.data_scale > 0 then "
					+ " ',' || a.data_scale "
					+ " else "
					+ " '' "
					+ " end || ')' "
					+ " end fieldType, "
					+ " case "
					+ " when b.table_name is not null then "
					+ " 'Y' "
					+ " else "
					+ " 'N' "
					+ " end KEYFLAG, "//是否主键
					+ " a.nullable nullflag "//是否为NULL
					+ " FROM " 
					+ dbaTabCols.substring(dbaTabCols.indexOf(".")+1)
					+ " a "
					+ " left join (SELECT UC.TABLE_NAME, UCC.COLUMN_NAME "
					+ " FROM " 
					+ dbaConstraints.substring(dbaConstraints.indexOf(".")+1)
					+ " UC, " 
					+ dbaConsColumns.substring(dbaConsColumns.indexOf(".")+1)
					+ " UCC "
					+ " WHERE UC.CONSTRAINT_NAME = UCC.CONSTRAINT_NAME "
					+ " AND CONSTRAINT_TYPE = 'P') b "
					+ " on a.table_name = b.table_name "
					+ " and a.column_name = b.column_name " + " LEFT JOIN "
					+ dbaColComments.substring(dbaColComments.indexOf(".")+1) 
					+ " c "
					+ " ON a.table_name = c.TABLE_NAME "
					+ " and a.OWNER = C.OWNER "
					+ " AND a.column_name = c.COLUMN_NAME " + " LEFT JOIN "
					+ dbaTabComments.substring(dbaTabComments.indexOf(".")+1)
					+ " d "
					+ " ON a.table_name = d.TABLE_NAME "
					+ " and a.OWNER = d.OWNER " + " where a.table_name not like '%==$0' and a.owner = ? ";
		}
		return sql;
	}

	/**
	 * @throws SQLException 
	  * 初始化数据，不同类型数据库中的函数不一致，无法统一SQL
	  * ，现通过代码实现（wmsys.wm_concat()），并保存到临时表中，
	  * 当执行完成后，删除该临时表
	  * @Title: initData  
	  * @Description: TODO 
	  * @param  
	  * @return void 
	  * @throws
	 */
	private void initUniqColumnData(String schName) throws SQLException {
		//创建临时存放表
		String sql = "create table "
				+ tmpTable
				+ "(table_owner varchar(128),table_name varchar(128),UNIQUE_index_name varchar(512),NONUNIQUE_index_name varchar(512),SRC_NME varchar(512))";
		log.info("Oracle【创建" + tmpTable + "临时表...】");
		creTable(sql);
		log.info("Oracle【采集uniqColumn数据...】");
		List<Map<String, String>> listMaps = getUniqColumnData(schName);
		log.info("Oracle【对uniqColumn数据做去重处理...】");
		//对数据进行类似于oracle的wmsys.wm_concat函数操作
		listMaps = concatUniqColumn(listMaps);
		log.info("Oracle【插入" + tmpTable + "临时表...】");
		//插入数据
		insUnqiColnumData(listMaps);
	}

	private void creTable(String sql) throws SQLException{
		try {
			getHsqlJdbcTemplate().executeQuery(sql);
		} catch (Exception e) {
			getHsqlJdbcTemplate().executeQuery("drop table "+tmpTable);
			getHsqlJdbcTemplate().executeQuery(sql);
		}
	}

	private void delTable() throws SQLException {
		log.info("删除" + tmpTable + "临时表...");
		String sql = " drop table " + tmpTable;
		try {
			getHsqlJdbcTemplate().executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void insUnqiColnumData(List<Map<String, String>> listMaps)
			throws SQLException {
		PreparedStatement ps = null;
		Connection conn = HSqlDB.getConnection();
		try {
			//获取数据库连接
			//设置提交为false，当数据处理完成后，一次性提交
			conn.setAutoCommit(false);
			for (Map<String, String> map : listMaps) {
				String sql = "insert into " + tmpTable + " values('"
						+ map.get("table_owner") + "','"
						+ map.get("table_name") + "','"
						+ map.get("UNIQUE_index_name") + "','"
						+ map.get("NONUNIQUE_index_name") + "','"
						+ map.get("SRC_NME") + "') ";
				ps = conn.prepareStatement(sql);
				ps.execute();
				ps.close();
			}
			//再一次提交数据
			conn.commit();
		} catch (Exception e) {
			//删除临时表
			log.info("插入临时表" + tmpTable + "数据失败，错误信息：" + e);
			// 及时关闭数据源连接
//			if (conn != null) {
//				conn.close();
//			}
			delTable();
		} finally {
			//处理完毕后，及时关掉数据库连接
//			if (conn != null) {
//				conn.close();
//			}
		}
	}

	/**
	  * 执行类似于oracle的wmsys.wm_concat函数操作
	  * @Title: concatUniqColumn  
	  * @Description: TODO 
	  * @param  listMaps
	  * @return void 
	  * @throws
	 */
	private List<Map<String, String>> concatUniqColumn(List<Map<String, String>> listMaps) {
		List<Map<String, String>> concatList = new ArrayList<Map<String,String>>();
			Map<String, String> concatMaps = new HashMap<String, String>();
			for (Map<String, String> map : listMaps) {
				String tableOwner = map.get("TABLE_OWNER");
				String tableName = map.get("TABLE_NAME");
				String SrcNme = map.get("SRC_NME");
				String uniq = map.get("UNIQUENESS");
				String idxName = map.get("INDEX_NAME");
				String columnName = map.get("COLUMN_NAME") == null ? "" : map.get("COLUMN_NAME");
				String srcNme = "";
				String uStr = "";
				//当ODS代码为null时，不对ODS代码进行ID唯一组装
				if(SrcNme!=null&&!"".equals(SrcNme)){
					srcNme = "##"+SrcNme;
				}
				//判断是否为唯一约束，加入标示符号
				if(uniq.equals("UNIQUE")){
					uStr = "#UNIQUE#"+idxName+"#"+columnName;
				}else{
					uStr = "#NONUNIQUE#"+idxName+"#"+columnName;
				}
				//判断是否已保存有该索引（schema、表、约束名称、ODS代码作为主键）
				if(concatMaps.containsKey(tableOwner+"##"+tableName+srcNme)){
					concatMaps.put(tableOwner+"##"+tableName+srcNme, 
							concatMaps.get(tableOwner+"##"+tableName+srcNme)+";"+uStr);
				}else{
					concatMaps.put(tableOwner+"##"+tableName+srcNme, uStr);
				}
			}
			//拆分主键分别存放
			for (Iterator<String> indId = concatMaps.keySet().iterator(); indId.hasNext();) {
				Map<String, String> concatMap = new HashMap<String, String>();
				String ind = indId.next();
				String [] indIds = ind.split("##");
				concatMap.put("table_owner", indIds[0]);
				concatMap.put("table_name", indIds[1]);
				String [] uStrs = concatMaps.get(ind).split(";");
				String unique = "";
				String nonunique = "";
				for (String uStr : uStrs) {
					if(uStr.startsWith("#UNIQUE#")){
						unique += uStr.replace("#UNIQUE#", "")+";";
					}else if(uStr.startsWith("#NONUNIQUE#")){
						nonunique += uStr.replace("#NONUNIQUE#", "")+";";
					}
				}
				concatMap.put("UNIQUE_index_name", getGroupIndex(unique));
				concatMap.put("NONUNIQUE_index_name", getGroupIndex(nonunique));
				if(indIds.length>2){
					concatMap.put("SRC_NME", indIds[2]);
				}else{
					concatMap.put("SRC_NME",null);
				}
				concatList.add(concatMap);
			}
			//清空
			listMaps.clear();
			concatMaps.clear();
		return concatList;
	}
	public String getGroupIndex(String ind){
		//不允许出现空值
		if(ind==null||"".equals(ind))
			return ind;
		String uStr = "";
		String cols [] = ind.split(";");
		Map<String, String> maps= new HashMap<String, String>();
		for (String col: cols) {
			String inds [] = col.split("#");
			if(inds.length<=1)
				continue;
			if(maps.containsKey(inds[0].trim())){
				maps.put(inds[0].trim(),maps.get(inds[0].trim())+"+"+inds[1]);
			}else{
				maps.put(inds[0].trim(),inds[1]);
			}
		}
		for (Iterator<String> map = maps.keySet().iterator(); map.hasNext();){
			if(uStr.equals(""))
				uStr = maps.get(map.next());
			else
				uStr = uStr+";"+maps.get(map.next());
		}
		return uStr;
	}
	/**
	 * @throws SQLException 
	  * 获取唯一列数据
	  * @Title: getUniqColumnData  
	  * @Description: TODO 
	  * @param @param schName
	  * @param @return 
	  * @return List<Map<String,String>> 
	  * @throws
	 */
	private List<Map<String, String>> getUniqColumnData(String schName) throws SQLException{
		String dbaIndColumns = properties.getProperty("DBA_IND_COLUMNS");
		String dbaIndxes = properties.getProperty("DBA_INDEXES");
		String whereSql = " and idx.owner = ? ";
		String [] param = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		String fieStr = ", '' SRC_NME";
		String ordStr = "";
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			param = new String[] { schName, SRC_NME };
			whereSql += " and idx.SRC_NME = idxc.SRC_NME and idx.SRC_NME = ? ";
			fieStr = ",idx.SRC_NME";
			ordStr = ",idx.SRC_NME";
		} else {
			param = new String[] { schName };
		}
		String sql = "					Select idx.table_owner, "
				+ "                       idx.table_name, "
				+ "                       idx.uniqueness, idx.INDEX_NAME, "
				+ "                       idxc.column_name "+fieStr
				+ "                 from "
				+ dbaIndxes.substring(dbaIndxes.indexOf(".")+1)
				+ " idx "
				+ "                 left join "
				+ dbaIndColumns.substring(dbaIndColumns.indexOf(".")+1)
				+ " idxc "
				+ "                   on idx.table_name = idxc.TABLE_NAME "
				+ "                  and idx.index_name = idxc.INDEX_NAME "
				+ "                  and idx.table_owner = idxc.table_owner "
				+ "                  and idx.owner = idxc.index_owner where 1=1 "
				+ whereSql + "                Group by idx.table_owner, "
				+ "                          idx.table_name,idx.INDEX_NAME, "
				+ "                          idx.uniqueness, "
				+ "                          idxc.column_name  "+ordStr;
			return getHsqlJdbcTemplate().queryForList(sql, param);
	}

	@SuppressWarnings("unchecked")
	protected List<TableVO> getTables(String schName, int start, int limit)
			throws SQLException {
		//在采集table字典前，需对uniq数据进行初始化临时表。
		try {
			log.info("开始采集表信息...");
			String sql = "";
			String [] param = null;
			//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
			if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
				sql = getOraSql("table", SRC_NME);
				param = new String[] { schName, SRC_NME };
			} else {
				sql = getOraSql("table");
				param = new String[] { schName };
			}
			//根据不同类型的数据，得到不同的分页查询SQL
			sql = getRownumSql(sql, start, limit);
			List<TableVO> lists = getHsqlJdbcTemplate().queryForListToTableVO(sql, param);
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


	@SuppressWarnings("unchecked")
	protected List<ColumnVO> getFields(String schName, int start, int limit)
			throws SQLException {
		log.info("开始采集表的字段信息...");
		String sql = "";
		String [] param = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql = getOraSql("column", SRC_NME);
			param = new String[] { schName, SRC_NME };
		} else {
			sql = getOraSql("column");
			param = new String[] { schName };
		}
		//根据不同类型的数据，得到不同的分页查询SQL
		sql = getRownumSql(sql, start, limit);
		List<ColumnVO> lists = super.getHsqlJdbcTemplate().queryForListToFieldVO(sql, param);
		if (lists == null) {
			return new ArrayList<ColumnVO>();
		}
		return lists;
	}

	public int getFieldSize(String schName) throws SQLException {
		log.info("获取表的字段信息长度...");
		String sql = "";
		String [] param = null;
		//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
		if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
			sql = getOraSql("column", SRC_NME);
			param = new String[] { schName, SRC_NME };
		} else {
			sql = getOraSql("column");
			param = new String[] { schName };
		}
		sql = "select count(1) from (" + sql + ") c ";
		return super.getHsqlJdbcTemplate().queryForInt(sql,param);
	}

	public int getTableSize(String schName) throws SQLException {
		try {
			//在采集table过程中，需对uniq数据进行初始化临时表。
			initUniqColumnData(schName);
			log.info("获取表级信息长度...");
			String sql = "";
			String [] param = null;
			//ODS条件判断，加入ODS系统代码 判断条件，如果不为空，则加入判断条件
			if (SRC_NME != null && !"".equals(SRC_NME.trim())) {
				sql = getOraSql("table", SRC_NME);
				param = new String[] { schName, SRC_NME };
			} else {
				sql = getOraSql("table");
				param = new String[] { schName };
			}
			sql = "select count(1) from (" + sql + ") c ";
			return super.getHsqlJdbcTemplate().queryForInt(sql,param);
		} catch (Exception e) {
			//当table数据采集完成后，需删除临时表
			delTable();
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
		rowNumSql = "select limit "+start+" "+limit+" * from ("+sql+") ";
		
//		if(conn!=null){
//			conn.close();
//		}
		return rowNumSql;
	}
	
	public void initHSQLDB(String schName) throws  Exception {
		// TODO Auto-generated method stub
		JiCaiHSqlDBUtil hsql = new JiCaiHSqlDBUtil();
		hsql.initSystemTable(properties, dbType, super.getJdbcTemplate().getDataSource().getConnection(), schName);
	}
}
