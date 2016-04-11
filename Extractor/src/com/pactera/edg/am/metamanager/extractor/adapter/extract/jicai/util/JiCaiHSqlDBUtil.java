/**    
  * @FileName: CGBHSqlDBUtil.java  
  * @Package:com.vibi.dgp.extractor.adapter.cgb.extract.util  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2012-10-12 上午10:06:11  
  * @version V1.0    
  */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** 
  * @ClassName: CGBHSqlDBUtil  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2012-10-12 上午10:06:11   
  */
public class JiCaiHSqlDBUtil {
	private static Log log = LogFactory.getLog(JiCaiHSqlDBUtil.class);
	/**
	  * 在每次采集数据字典时，首先需对内存数据库的数据进行初始化操作 
	  * 内存数据库数据加载方式为文件加载方式
	  * @Title: initSystemTable  
	  * @param @param properties
	  * @param @param dbType
	  * @param @param conn
	  * @param @param schName
	  * @param @throws Exception 
	  * @return void 
	  * @throws
	 */
	public void initSystemTable(Properties properties, String dbType,Connection conn,String schName) throws Exception{
		//初始化表
		initTables(properties, dbType);
		//初始化表数据
		initTabDatas(properties, dbType, conn, schName);
	}
	
	
	private void initTables(Properties properties, String dbType) throws Exception{
		try {
			File f = new File(
					HSqlDB.getCgbPro().getProperty("script_file_path"));
			BufferedReader br = new BufferedReader(new FileReader(f));
			StringBuffer sb = new StringBuffer();
			String aline;
			while ((aline = br.readLine()) != null) {//按行读取文本
				sb.append(aline + "\n");
				if (aline.equals("CREATE SCHEMA PUBLIC AUTHORIZATION DBA")) {
					sb.append("SET SCHEMA PUBLIC\n");
					if (dbType != null) {
						if (dbType.toLowerCase().equals("db2")) {
							systemTableToDB2(properties, sb);
						} else if (dbType.toLowerCase().equals("oracle")) {
							systemTableToOracle(properties, sb);
						} else if (dbType.toLowerCase().equals("sqlserver")) {
							systemTableToSQLServer(properties, sb);
						}
					}
				}
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(
					HSqlDB.getCgbPro().getProperty("init_script_file_path")));
			bw.write(sb.toString());
			bw.flush();
			bw.close();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.info("初始化【" + dbType + "】系统表过程出现" + e.getMessage() + "错误！");
			throw e;
		}

	}
	
	private void initTabDatas(Properties properties, String dbType,
			Connection conn,String schName) throws Exception {
		if(dbType!=null){
			if(dbType.toLowerCase().equals("db2"))
				systemTabDataToDB2(properties, conn, schName);
			else if (dbType.toLowerCase().equals("oracle"))
				systemTabDataToOracle(properties, conn, schName);
			else if (dbType.toLowerCase().equals("sqlserver"))
				systemTabDataToSQLServer(properties, conn, schName);
		}
	}
	/**
	 * 初始化DB2数据字典表数据信息
	  * @Title: systemTabDataToDB2  
	  * @param @param properties
	  * @param @param conn
	  * @param @param schName
	  * @param @throws IOException
	  * @param @throws SQLException 
	  * @return void 
	  * @throws
	 */
	private void systemTabDataToDB2(Properties properties, Connection conn,String schName) throws IOException, SQLException {
		String srcNme = properties.getProperty("SRC_NME");
		BufferedWriter out = null;
		try {
			//获取内存数据库所存放的script文件目录内容
			out = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(
							HSqlDB.getCgbPro().getProperty("init_script_file_path")), true)));
			out.write("SET SCHEMA PUBLIC\n");
			/**
			 * 获取字段数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			columnsToDB2(properties.getProperty("SYSCAT.COLUMNS"), schName, srcNme, conn, out);
			/**
			 * 获取表级数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			tablesToDB2(properties.getProperty("SYSCAT.TABLES"), schName, srcNme, conn, out);
			/**
			 * 获取索引数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			indexesToDB2(properties.getProperty("SYSCAT.INDEXES"), schName, srcNme, conn, out);
		} catch (Exception e) {
			e.printStackTrace();
			log.info("初始化DB2表数据时出现" + e.getMessage() + "错误！");
		}finally{
			if(out != null){
				out.flush();
				out.close();
			}
			if(conn != null)
				conn.close();
		}
	}

	private void systemTabDataToSQLServer(Properties properties, Connection conn,String schName) throws Exception {
		String srcNme = properties.getProperty("SRC_NME");
		BufferedWriter out = null;
		try {
			//获取内存数据库所存放的script文件目录内容
			out = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(
							HSqlDB.getCgbPro().getProperty("init_script_file_path")), true)));
			out.write("SET SCHEMA PUBLIC\n");
			/**
			 * 获取sysobjects数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			sysobjectsToSql(properties, schName, srcNme, conn, out);
			/**
			 * 获取sysindexes数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			sysindexesToSql(properties, schName, srcNme, conn, out);
			/**
			 * 获取sysindexkeys数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			sysindexkeysToSql(properties, schName, srcNme, conn, out);
			/**
			 * 获取syscolumns数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			syscolumnsToSql(properties, schName, srcNme, conn, out);
			/**
			 * 获取systypes数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			systypesToSql(properties, schName, srcNme, conn, out);
			/**
			 * 获取extendedProperties数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			extendedPropertiesToSql(properties, schName, srcNme, conn, out);
			/**
			 * 获取indexes数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			indexesToSql(properties, schName, srcNme, conn, out);
			/**
			 * 获取tables数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			tablesToSql(properties, schName, srcNme, conn, out);
		} catch (Exception e) {
			e.printStackTrace();
			log.info("初始化SQL表数据时出现" + e.getMessage() + "错误！");
		}finally{
			if(out != null){
				out.flush();
				out.close();
			}
			if(conn != null)
				conn.close();
		}
	}
	
	private void systemTabDataToOracle(Properties properties, Connection conn,String schName) throws IOException, SQLException {
		String srcNme = properties.getProperty("SRC_NME");
		BufferedWriter out = null;
		try {
			//获取内存数据库所存放的script文件目录内容
			out = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(
							HSqlDB.getCgbPro().getProperty("init_script_file_path")), true)));
			out.write("SET SCHEMA PUBLIC\n");
			/**
			 * 获取列信息数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			dbaTabColsToOracle(properties.getProperty("DBA_TAB_COLS"), schName, srcNme, conn, out);
			/**
			 * 获取表约束字段视图数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			dbaConsColumnsToOracle(properties.getProperty("DBA_CONS_COLUMNS"), schName, srcNme, conn, out);
			/**
			 * 获取表注释数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			dbaTabCommentsToOracle(properties.getProperty("DBA_TAB_COMMENTS"), schName, srcNme, conn, out);
			/**
			 * 获取列注释数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			dbaColCommentsToOracle(properties.getProperty("DBA_COL_COMMENTS"), schName, srcNme, conn, out);
			/**
			 * 获取索引列数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			dbaIndColumnsToOracle(properties.getProperty("DBA_IND_COLUMNS"), schName, srcNme, conn, out);
			/**
			 * 获取约束信息数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			dbaConstraintsToOracle(properties.getProperty("DBA_CONSTRAINTS"), schName, srcNme, conn, out);
			
			/**
			 * 获取索引数据字典表所需的必须字段数据，写入内存数据库的script文件中
			 */
			dbaIndexesToOracle(properties.getProperty("DBA_INDEXES"), schName, srcNme, conn, out);
			
		} catch (Exception e) {
			e.printStackTrace();
			log.info("初始化Oracle表数据时出现" + e.getMessage() + "错误！");
		}finally{
			if(out != null){
				out.flush();
				out.close();
			}
			if(conn != null)
				conn.close();
		}
	}
	
	private void tablesToDB2(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql =  "select "+odsCol+" TABSCHEMA ,TABNAME ,REMARKS  from "
					+ tabName
					+ " where TABSCHEMA = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (";
						if (srcNme != null && !"".equals(srcNme.trim())) {
							str += "'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
							"'"+replaceAll(rs.getString("SRC_NME"))+"',";
						}else{
							str+="'','',";
						}
						str +="'"+replaceAll(rs.getString("TABSCHEMA"))+"'," +
						"'"+replaceAll(rs.getString("TABNAME"))+"'," +
						"'"+replaceAll(rs.getString("REMARKS"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void columnsToDB2(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" TABSCHEMA ,TABNAME ,COLNAME,TYPENAME,LENGTH,SCALE,NULLS,KEYSEQ,REMARKS,COLNO  from "
					+ tabName
					+ " where TABSCHEMA = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
					str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"',";
				}else{
					str+="'','',";
				}
				str+="'"+replaceAll(rs.getString("TABSCHEMA"))+"'," +
						"'"+replaceAll(rs.getString("TABNAME"))+"'," +
						"'"+replaceAll(rs.getString("COLNAME"))+"'," +
						"'"+replaceAll(rs.getString("TYPENAME"))+"'," +
						""+rs.getInt("LENGTH")+"," +
						""+rs.getInt("SCALE")+"," +
						"'"+replaceAll(rs.getString("NULLS"))+"'," +
						""+rs.getInt("KEYSEQ")+"," +
						"'"+replaceAll(rs.getString("REMARKS"))+"'," +
						""+rs.getInt("COLNO")+"" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void indexesToDB2(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" TABSCHEMA,TABNAME,COLNAMES,UNIQUERULE from "
					+ tabName
					+ " where TABSCHEMA = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"',";
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("TABSCHEMA"))+"'," +
						"'"+replaceAll(rs.getString("TABNAME"))+"'," +
						"'"+replaceAll(rs.getString("COLNAMES"))+"'," +
						"'"+replaceAll(rs.getString("UNIQUERULE"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void dbaTabColsToOracle(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" OWNER , TABLE_NAME , COLUMN_NAME , DATA_TYPE , DATA_LENGTH ,DATA_PRECISION, DATA_SCALE , NULLABLE , COLUMN_ID    from "
					+ tabName
					+ " where OWNER = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and SRC_NME = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str += "'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"',";
				}else{
					str+="'','',";
				}
						str += "'"+replaceAll(rs.getString("OWNER"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_NAME"))+"'," +
						"'"+replaceAll(rs.getString("COLUMN_NAME"))+"'," +
						"'"+replaceAll(rs.getString("DATA_TYPE"))+"'," +
						""+rs.getInt("DATA_LENGTH")+"," +
						""+rs.getInt("DATA_PRECISION")+"," +
						""+rs.getInt("DATA_SCALE")+"," +
						"'"+replaceAll(rs.getString("NULLABLE"))+"'," +
						""+rs.getInt("COLUMN_ID")+"" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void dbaConsColumnsToOracle(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" CONSTRAINT_NAME , COLUMN_NAME ,OWNER  from "
					+ tabName
					+ " where OWNER = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("CONSTRAINT_NAME"))+"'," +
						"'"+replaceAll(rs.getString("COLUMN_NAME"))+"'," +
						"'"+replaceAll(rs.getString("OWNER"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void dbaTabCommentsToOracle(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" OWNER,TABLE_NAME,TABLE_TYPE,COMMENTS from "
					+ tabName
					+ " where OWNER = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("OWNER"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_NAME"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_TYPE"))+"'," +
						"'"+replaceAll(rs.getString("COMMENTS"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void dbaColCommentsToOracle(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" OWNER,TABLE_NAME,COLUMN_NAME,COMMENTS from "
					+ tabName
					+ " where OWNER = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("OWNER"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_NAME"))+"'," +
						"'"+replaceAll(rs.getString("COLUMN_NAME"))+"'," +
						"'"+replaceAll(rs.getString("COMMENTS"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void dbaIndColumnsToOracle(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" INDEX_OWNER,INDEX_NAME,TABLE_OWNER,TABLE_NAME,COLUMN_NAME from "
					+ tabName
					+ " where index_owner = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
					if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
					}else{
						str+="'','',";
					}
						str+="'"+replaceAll(rs.getString("INDEX_OWNER"))+"'," +
						"'"+replaceAll(rs.getString("INDEX_NAME"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_OWNER"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_NAME"))+"'," +
						"'"+replaceAll(rs.getString("COLUMN_NAME"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void dbaConstraintsToOracle(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" OWNER,CONSTRAINT_NAME,CONSTRAINT_TYPE,TABLE_NAME,R_CONSTRAINT_NAME from "
					+ tabName
					+ " where OWNER = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("OWNER"))+"'," +
						"'"+replaceAll(rs.getString("CONSTRAINT_NAME"))+"'," +
						"'"+replaceAll(rs.getString("CONSTRAINT_TYPE"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_NAME"))+"'," +
						"'"+replaceAll(rs.getString("R_CONSTRAINT_NAME"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	private void dbaIndexesToOracle(String tabName,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " DBM_TYP,SRC_NME, ";
			}
			String sql = "select "+odsCol+" OWNER,INDEX_NAME,TABLE_OWNER,TABLE_NAME ,UNIQUENESS  from "
					+ tabName
					+ " where OWNER = '"+schName+"' ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and src_nme = '"+srcNme+"'";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+tabName+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("OWNER"))+"'," +
						"'"+replaceAll(rs.getString("INDEX_NAME"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_OWNER"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_NAME"))+"'," +
						"'"+replaceAll(rs.getString("UNIQUENESS"))+"'" +
						")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	

	private void sysobjectsToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t1.DBM_TYP,t1.SRC_NME, ";
			}
			String sql = "select "+odsCol+"t1.NAME,t1.ID,t1.XTYPE,t1.PARENT_OBJ from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2 " 
					+"where  (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " //and t2.table_catalog = 'test'
					+" and table_name = t1.name";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"'  and t2.SRC_NME = t1.SRC_NME ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("SYSOBJECTS").substring(properties.getProperty("SYSOBJECTS").indexOf(".")+1)+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("NAME"))+"'," +
						""+rs.getInt("ID")+"," +
						"'"+replaceAll(rs.getString("XTYPE"))+"'," +
						""+rs.getInt("PARENT_OBJ")+")" ;
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}

	private void sysindexesToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t3.DBM_TYP,t3.SRC_NME, ";
			}
			String sql = "select "+odsCol+"t3.INDID,t3.NAME " 
					+"from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2, " 
					+ properties.getProperty("SYSINDEXES")+" t3 " 
					+"where t1.id = t3.id " 
					+"and (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " // and t2.table_catalog = 'master' 
					+"and t2.table_name = t1.name ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"'  and t2.SRC_NME = t1.SRC_NME and t2.SRC_NME = t3.SRC_NME ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("SYSINDEXES").substring(properties.getProperty("SYSINDEXES").indexOf(".")+1)+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
					str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
					str+=rs.getInt("INDID")+"," +
						"'"+replaceAll(rs.getString("NAME"))+"')";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void sysindexkeysToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t3.DBM_TYP,t3.SRC_NME, ";
			}
			String sql = "select "+odsCol+"t3.SRC_NME,t3.ID,t3.COLID,t3.INDID " 
					+"from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2, " 
					+ properties.getProperty("SYSINDEXKEYS")+" t3 " 
					+"where t1.id = t3.id " 
					+"and  (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " // and t2.table_catalog = 'master' 
					+"and t2.table_name = t1.name ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"'  and t2.SRC_NME = t1.SRC_NME and t2.SRC_NME = t3.SRC_NME ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("SYSINDEXKEYS").substring(properties.getProperty("SYSINDEXKEYS").indexOf(".")+1)+" VALUES (";
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						 str+=rs.getInt("ID")+"," +
							+rs.getInt("COLID")+","+
							+rs.getInt("INDID")+")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void syscolumnsToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t3.DBM_TYP,t3.SRC_NME, ";
			}
			String sql = "select "+odsCol+"t3.NAME,t3.ID,t3.XUSERTYPE,t3.COLID,t3.CDEFAULT,t3.COLORDER,t3.ISNULLABLE,t3.PREC,t3.SCALE " 
					+"from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2, " 
					+ properties.getProperty("SYSCOLUMNS")+" t3 " 
					+"where t1.id = t3.id " 
					+"and  (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " // and t2.table_catalog = 'master' 
					+"and t2.table_name = t1.name ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"'  and t2.SRC_NME = t1.SRC_NME and t2.SRC_NME = t3.SRC_NME ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("SYSCOLUMNS").substring(properties.getProperty("SYSCOLUMNS").indexOf(".")+1)+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("NAME"))+"'," +
						""+rs.getInt("ID")+"," +
						""+rs.getInt("XUSERTYPE")+"," +
						""+rs.getInt("COLID")+"," +
						""+rs.getInt("CDEFAULT")+"," +
						""+rs.getInt("COLORDER")+"," +
						""+rs.getInt("ISNULLABLE")+","+
						""+rs.getInt("PREC")+","+
						""+rs.getInt("SCALE")+")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void systypesToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t4.DBM_TYP,t4.SRC_NME, ";
			}
			String sql = "select distinct "+odsCol+"t4.NAME,t4.XUSERTYPE " 
					+"from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2, " 
					+ properties.getProperty("SYSCOLUMNS")+" t3 ," 
					+ properties.getProperty("SYSTYPES")+" t4 " 
					+"where t3.xusertype = t4.xusertype and t1.id = t3.id " 
					+"and  (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " // and t2.table_catalog = 'master' 
					+"and t2.table_name = t1.name ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"' and t2.SRC_NME = t1.SRC_NME and t2.SRC_NME = t3.SRC_NME and t3.SRC_NME = t4.SRC_NME ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("SYSTYPES").substring(properties.getProperty("SYSTYPES").indexOf(".")+1)+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("NAME"))+"'," +
						""+rs.getInt("XUSERTYPE")+")";
			    str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void extendedPropertiesToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t3.DBM_TYP,t3.SRC_NME, ";
			}
			String sql = "select "+odsCol+"t3.CLASS,t3.MINOR_ID,t3.VALUE,t3.MAJOR_ID " 
					+"from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2, " 
					+ properties.getProperty("EXTENDED_PROPERTIES")+" t3 " 
					+"where t1.id = t3.major_id " 
					+"and  (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " // and t2.table_catalog = 'master' 
					+"and t2.table_name = t1.name ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"'  and t2.SRC_NME = t1.SRC_NME and t2.SRC_NME = t3.SRC_NME ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("EXTENDED_PROPERTIES").substring(properties.getProperty("EXTENDED_PROPERTIES").indexOf(".")+1)+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+=""+rs.getInt("CLASS")+"," +
						""+rs.getInt("MINOR_ID")+"," +
						"'"+replaceAll(rs.getString("VALUE"))+"',"+
						""+rs.getInt("MAJOR_ID")+")";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void indexesToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t3.DBM_TYP,t3.SRC_NME, ";
			}
			String sql = "select "+odsCol+"t3.OBJECT_ID,t3.IS_UNIQUE,t3.NAME " 
					+"from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2, " 
					+ properties.getProperty("INDEXES")+" t3 " 
					+"where t1.id = t3.object_id " 
					+"and  (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " // and t2.table_catalog = 'master' 
					+"and t2.table_name = t1.name ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"'  and t2.SRC_NME = t1.SRC_NME and t2.SRC_NME = t3.SRC_NME ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("INDEXES").substring(properties.getProperty("INDEXES").indexOf(".")+1)+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+=""+rs.getInt("OBJECT_ID")+"," +
						""+rs.getInt("IS_UNIQUE")+","+
						"'"+replaceAll(rs.getString("NAME"))+"')";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void tablesToSql(Properties properties,String schName,String srcNme,Connection conn,BufferedWriter out) throws Exception{
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String odsCol = "";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				odsCol = " t2.DBM_TYP,t2.SRC_NME, ";
			}
			String sql = "select "+odsCol+"t2.TABLE_CATALOG,t2.TABLE_SCHEMA,t2.TABLE_NAME,t2.TABLE_TYPE " 
					+"from " 
					+ properties.getProperty("SYSOBJECTS")+" t1 ," 
					+ properties.getProperty("TABLES")+" t2 " 
					+" where (t2.table_schema = '"+schName.toLowerCase()+"' or t2.table_schema = '"+schName.toUpperCase()+"') " // and t2.table_catalog = 'master' 
					+"and t2.table_name = t1.name ";
			if (srcNme != null && !"".equals(srcNme.trim())) {
				sql += " and t1.SRC_NME = '"+srcNme+"'  and t2.SRC_NME = t1.SRC_NME  ";
			}
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				String str = "INSERT INTO "+properties.getProperty("TABLES").substring(properties.getProperty("TABLES").indexOf(".")+1)+" VALUES (" ;
				if (srcNme != null && !"".equals(srcNme.trim())) {
						str+="'"+replaceAll(rs.getString("DBM_TYP"))+"'," +
						"'"+replaceAll(rs.getString("SRC_NME"))+"'," ;
				}else{
					str+="'','',";
				}
						str+="'"+replaceAll(rs.getString("TABLE_CATALOG"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_SCHEMA"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_NAME"))+"'," +
						"'"+replaceAll(rs.getString("TABLE_TYPE"))+"')";
				str = str.replaceAll("'null'","NULL")+"\n";
				out.write(str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(ps != null)
				ps.close();
		}
	}
	
	private void systemTableToDB2(Properties properties, StringBuffer sb) {
		sb.append("CREATE TABLE "
				+ properties.getProperty("SYSCAT.COLUMNS").substring(properties.getProperty("SYSCAT.COLUMNS").indexOf(".")+1)
				+ "(DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),TABSCHEMA VARCHAR(128),TABNAME VARCHAR(128),COLNAME VARCHAR(128),TYPENAME VARCHAR(128),LENGTH INTEGER,SCALE SMALLINT,NULLS CHARACTER(1),KEYSEQ SMALLINT,REMARKS VARCHAR(254),COLNO SMALLINT)\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("SYSCAT.TABLES").substring(properties.getProperty("SYSCAT.TABLES").indexOf(".")+1)
				+ "(DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),TABSCHEMA VARCHAR(128),TABNAME VARCHAR(128),REMARKS VARCHAR(254),TYPE CHAR(1))\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("SYSCAT.INDEXES").substring(properties.getProperty("SYSCAT.INDEXES").indexOf(".")+1)
				+ "(DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),TABSCHEMA VARCHAR(128),TABNAME VARCHAR(128),COLNAMES VARCHAR(640),UNIQUERULE CHARACTER(1))\n");
	}

	private void systemTableToOracle(Properties properties, StringBuffer sb) {
		sb.append("CREATE TABLE "
				+ properties.getProperty("DBA_TAB_COLS").substring(properties.getProperty("DBA_TAB_COLS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),OWNER VARCHAR(30), TABLE_NAME VARCHAR(30), COLUMN_NAME VARCHAR(30), DATA_TYPE VARCHAR(106), DATA_LENGTH INTEGER,DATA_PRECISION INTEGER, DATA_SCALE INTEGER, NULLABLE VARCHAR(1), COLUMN_ID INTEGER)\n");
		sb.append("CREATE TABLE " + properties.getProperty("DBA_CONS_COLUMNS").substring(properties.getProperty("DBA_CONS_COLUMNS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),CONSTRAINT_NAME VARCHAR(30), COLUMN_NAME VARCHAR(4000),OWNER VARCHAR(30))\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("DBA_TAB_COMMENTS").substring(properties.getProperty("DBA_TAB_COMMENTS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),OWNER VARCHAR(30),TABLE_NAME VARCHAR(30),TABLE_TYPE VARCHAR(11),COMMENTS VARCHAR(4000))\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("DBA_COL_COMMENTS").substring(properties.getProperty("DBA_COL_COMMENTS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),OWNER VARCHAR(30),TABLE_NAME VARCHAR(30),COLUMN_NAME VARCHAR(30),COMMENTS VARCHAR(4000))\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("DBA_IND_COLUMNS").substring(properties.getProperty("DBA_IND_COLUMNS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),INDEX_OWNER VARCHAR(30),INDEX_NAME VARCHAR(30),TABLE_OWNER VARCHAR(30),TABLE_NAME VARCHAR(30),COLUMN_NAME VARCHAR(40000))\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("DBA_CONSTRAINTS").substring(properties.getProperty("DBA_CONSTRAINTS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),OWNER VARCHAR(30),CONSTRAINT_NAME VARCHAR(30),CONSTRAINT_TYPE VARCHAR(1),TABLE_NAME VARCHAR(30),R_CONSTRAINT_NAME VARCHAR(30))\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("DBA_INDEXES").substring(properties.getProperty("DBA_INDEXES").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),OWNER VARCHAR(30),INDEX_NAME VARCHAR(30),TABLE_OWNER VARCHAR(30),TABLE_NAME VARCHAR(30),UNIQUENESS VARCHAR(12))\n");
	}

	private void systemTableToSQLServer(Properties properties, StringBuffer sb) {
		sb.append("CREATE TABLE " + properties.getProperty("SYSOBJECTS").substring(properties.getProperty("SYSOBJECTS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),NAME	VARCHAR(128),ID	INTEGER,XTYPE	CHAR(2),PARENT_OBJ	INTEGER)\n");
		sb.append("CREATE TABLE " + properties.getProperty("SYSINDEXES").substring(properties.getProperty("SYSINDEXES").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),INDID	SMALLINT,NAME	VARCHAR(128),ID INTEGER)\n");
		sb.append("CREATE TABLE " + properties.getProperty("SYSINDEXKEYS").substring(properties.getProperty("SYSINDEXKEYS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),ID INTEGER,COLID	SMALLINT,INDID INTEGER)\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("SYSCOLUMNS").substring(properties.getProperty("SYSCOLUMNS").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),NAME	VARCHAR(128),ID	INTEGER,XUSERTYPE	SMALLINT,COLID	SMALLINT,CDEFAULT	INTEGER,COLORDER	SMALLINT,ISNULLABLE	INTEGER,PREC SMALLINT,SCALE SMALLINT)\n");
		sb.append("CREATE TABLE " + properties.getProperty("SYSTYPES").substring(properties.getProperty("SYSTYPES").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),NAME	VARCHAR(128),XUSERTYPE	SMALLINT)\n");
		//		sb.append("CREATE TABLE " + properties.getProperty("SYSCOMMENTS")
		//				+ " (ID	INT)\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("EXTENDED_PROPERTIES").substring(properties.getProperty("EXTENDED_PROPERTIES").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),CLASS	INTEGER,MINOR_ID	INTEGER,VALUE	VARCHAR(512),MAJOR_ID INTEGER)\n");
		sb.append("CREATE TABLE " + properties.getProperty("INDEXES").substring(properties.getProperty("INDEXES").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),OBJECT_ID	INTEGER,IS_UNIQUE	INTEGER,NAME VARCHAR(128))\n");
		sb.append("CREATE TABLE "
				+ properties.getProperty("TABLES").substring(properties.getProperty("TABLES").indexOf(".")+1)
				+ " (DBM_TYP VARCHAR(20),SRC_NME VARCHAR(20),TABLE_CATALOG	VARCHAR(128),TABLE_SCHEMA	VARCHAR(128),TABLE_NAME	VARCHAR(128),TABLE_TYPE	VARCHAR(10))\n");
	}
	
	public String replaceAll(String str) throws UnsupportedEncodingException{
		if(str==null){
			return "null";
		}
		str = str.replace(" ", "");
		str = str.replaceAll("'null'", "NULL");
		str = str.replaceAll("'", "\"");
		str = str.replace("\n", " ");
		str = str.replaceAll("%", "％");
		int n=0;
		for(int i=0; i<str.length(); i++) {
			n = (int)str.charAt(i);
			if((19968 <= n && n <40623) || n == 0 ) {
				str = URLEncoder.encode(str,"UTF-8").replace("%00", "");
				break;
			}
		}
		return str;
	}
}
