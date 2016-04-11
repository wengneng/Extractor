/**    
  * @FileName: HSqlDBDaoImpl.java  
  * @Package:com.vibi.dgp.extractor.adapter.cgb.extract.db.impl  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2012-10-10 上午11:47:33  
  * @version V1.0    
  */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.impl;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.JiCaiHSqlDBDao;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.util.HSqlDB;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;

/** 
  * @ClassName: HSqlDBDaoImpl  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2012-10-10 上午11:47:33   
  */
public class JiCaiHSqlDBDaoImpl implements JiCaiHSqlDBDao {

	/* (non-Javadoc)  
	  * @param sql
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.HSqlDBDao#executeQuery(java.lang.String)  
	  */
	public int executeQuery(String sql) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;
		int num = 0;
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			num = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
		}
		return num;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @param param
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.HSqlDBDao#executeQuery(java.lang.String, java.lang.String[])  
	  */
	public int executeQuery(String sql, String[] param) throws SQLException {
		PreparedStatement ps = null;
		int num = 0;
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			int i = 1 ;
			for (String par : param) {
				ps.setString(i, par);
				i++;
			}
			num = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
		}
		return num;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.HSqlDBDao#queryForList(java.lang.String)  
	  */
	public List<?> queryForList(String sql) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> dataMap = new HashMap<String, Object>();
				int count = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= count; i++) {
					String colName = rs.getMetaData().getColumnName(i);
					Object obj = rs.getObject(colName);
					dataMap.put(colName, obj);
				}
				data.add(dataMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return data;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @param param
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.HSqlDBDao#queryForList(java.lang.String, java.lang.String[])  
	  */
	public List<?> queryForList(String sql, String[] param) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			int num = 1 ;
			for (String par : param) {
				ps.setString(num, par);
				num++;
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> dataMap = new HashMap<String, Object>();
				int count = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= count; i++) {
					String colName = rs.getMetaData().getColumnName(i);
					Object obj = rs.getObject(colName);
					dataMap.put(colName, obj);
				}
				data.add(dataMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return data;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.HSqlDBDao#query(java.lang.String)  
	  */
	public Map query(String sql) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, Object> dataMap = new HashMap<String, Object>();
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				int count = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= count; i++) {
					String colName = rs.getMetaData().getColumnName(i);
					Object obj = rs.getObject(colName);
					dataMap.put(colName, obj);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return dataMap;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @param param
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.HSqlDBDao#query(java.lang.String, java.lang.String[])  
	  */
	public Map query(String sql, String[] param) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, Object> dataMap = new HashMap<String, Object>();
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			int num = 1 ;
			for (String par : param) {
				ps.setString(num, par);
				num++;
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				int count = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= count; i++) {
					String colName = rs.getMetaData().getColumnName(i);
					Object obj = rs.getObject(colName);
					dataMap.put(colName, obj);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return dataMap;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @param param
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.CGBHSqlDBDao#queryForListToTableVO(java.lang.String, java.lang.String[])  
	  */
	public List<TableVO> queryForListToTableVO(String sql, String[] param)
			throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<TableVO> data = new ArrayList<TableVO>();
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			int num = 1 ;
			for (String par : param) {
				ps.setString(num, par);
				num++;
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				TableVO table = new TableVO();
				table.setSysName(replaceAll(rs.getString("sysName")));
				table.seteSysName(replaceAll(rs.getString("ESysName")));
				table.setDbName(replaceAll(rs.getString("dbName")));
				table.setTableName(replaceAll(rs.getString("tableName")));
				table.seteTableName(replaceAll(rs.getString("eTableName")));
				table.setDesc(replaceAll(rs.getString("des")));
				table.setTableType(replaceAll(rs.getString("table_type")));
				table.setIsPk(replaceAll(rs.getString("isPk")));
				table.setUniqueIndexName(rs.getString("UNIQUE_index_name"));
				table.setNonuniqueIndexName(rs
						.getString("NONUNIQUE_index_name"));
				data.add(table);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return data;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @param param
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.CGBHSqlDBDao#queryForListToFieldVO(java.lang.String, java.lang.String[])  
	  */
	public List<ColumnVO> queryForListToFieldVO(String sql, String[] param)
			throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
 		List<ColumnVO> data = new ArrayList<ColumnVO>();
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			int num = 1 ;
			for (String par : param) {
				ps.setString(num, par);
				num++;
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				ColumnVO field = new ColumnVO();
				field.setSysName(replaceAll(rs.getString("sysName")));
				field.seteSysName(replaceAll(rs.getString("ESysName")));
				field.setDbName(replaceAll(rs.getString("dbName")));
				field.setTableName(replaceAll(rs.getString("tableName")));
				field.seteTableName(replaceAll(rs.getString("eTableName")));
				field.setFieldOrd(replaceAll(rs.getString("fieldOrd")));
				field.setFieldName(replaceAll(rs.getString("fieldNme")));
				field.seteFieldName(replaceAll(rs.getString("eFieldName")));
				field.setKeyFlag(replaceAll(rs.getString("keyFlag")));
				field.setNullFlag(replaceAll(rs.getString("nullFlag")));
				field.setFieldType(replaceAll(rs.getString("fieldType")));
				data.add(field);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return data;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.CGBHSqlDBDao#queryForInt(java.lang.String)  
	  */
	public int queryForInt(String sql) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		int num = 0;
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				num = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return num;
	}

	/* (non-Javadoc)  
	  * @param sql
	  * @param param
	  * @return  
	  * @see com.vibi.dgp.extractor.adapter.cgb.extract.db.CGBHSqlDBDao#queryForInt(java.lang.String, java.lang.String[])  
	  */
	public int queryForInt(String sql, String[] param) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		int num = 0;
		try {
			Connection conn = HSqlDB.getConnection();
			ps = conn.prepareStatement(sql);
			int i = 1 ;
			for (String par : param) {
				ps.setString(i, par);
				i++;
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				num = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null)
				ps.close();
			if (rs != null)
				rs.close();
		}
		return num;
	}
	public String replaceAll(String str) throws UnsupportedEncodingException{
		if(str==null){
			return str;
		}
		str = URLDecoder.decode(str,"UTF-8");
		return str;
	}
	
}
