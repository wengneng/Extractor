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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.db.helper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.TdMacro;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;

public class TeradataExtractHelper {
	/**
	 * Teradata数据库的字段类型及其缩写
	 */
	public static final Map<String, String> DATA_TYPE = new HashMap<String, String>();
	static {
		DATA_TYPE.put("AT", "ANSI Time");
		DATA_TYPE.put("BF", "BYTE"); // Byte Fixed
		DATA_TYPE.put("BO", "BLOB"); // Byte Large Object
		DATA_TYPE.put("BS", "Binary String(family type)");
		DATA_TYPE.put("BV", "VARBYTE"); // Byte Varying
		DATA_TYPE.put("CF", "CHAR"); // Character Fixed Latin
		DATA_TYPE.put("CO", "CLOB"); // Character Large Object
		DATA_TYPE.put("CS", "Character String (family type)");
		DATA_TYPE.put("CV", "VARCHAR"); // Character Varying Latin
		DATA_TYPE.put("D", "DECIMAL");
		DATA_TYPE.put("DA", "Date");
		DATA_TYPE.put("DH", "Day-Hour");
		DATA_TYPE.put("DI", "Day");
		DATA_TYPE.put("DM", "Day-Minute");
		DATA_TYPE.put("DS", "Day-Second");
		DATA_TYPE.put("DT", "Date Tag (family tag)");
		DATA_TYPE.put("DY", "Day");
		DATA_TYPE.put("F", "FLOAT"); // Real or Floating Point
		DATA_TYPE.put("HM", "Hour-Minute");
		DATA_TYPE.put("HR", "Hour");
		DATA_TYPE.put("HS", "Hour-Second");
		DATA_TYPE.put("I", "INTEGER"); // 4 Byte Integer
		DATA_TYPE.put("I1", "BYTEINT"); // 1 Byte Integer
		DATA_TYPE.put("I2", "SMALLINT"); // 2 Byte Integer
		DATA_TYPE.put("I8", "8 Byte Integer"); // 8 Byte Integer
		DATA_TYPE.put("LF", "Pre-TD12.0 Character Fixed Locale (Kanji1 or Latin)");
		DATA_TYPE.put("LV", "Pre-TD12.0 Character Varying Locale (Kanji1 or Latin)");
		DATA_TYPE.put("MI", "Minute");
		DATA_TYPE.put("MO", "Month");
		DATA_TYPE.put("MS", "Minute to Second");
		DATA_TYPE.put("NM", "Number Tag (family code)");
		DATA_TYPE.put("PD", "PERIOD(DATE)");
		DATA_TYPE.put("PM", "PERIOD(TIME STAMP(n) WITH TIME ZONE)");
		DATA_TYPE.put("PS", "PERIOD(TIME STAMP(n))");
		DATA_TYPE.put("PT", "PERIOD(TIME(n))");
		DATA_TYPE.put("PZ", "PERIOD(TIME(n) WITH TIME ZONE)");
		DATA_TYPE.put("SC", "Second");
		DATA_TYPE.put("SZ", "Timestamp with Time Zone");
		DATA_TYPE.put("TM", "Time Tag (family code)");
		DATA_TYPE.put("TS", "TIMESTAMP"); // Timestamp without Time Zone
		DATA_TYPE.put("TZ", "ANSI Time with Time Zone");
		DATA_TYPE.put("UF", "Character Fixed Unicode");
		DATA_TYPE.put("UT", "User Defined Type");
		DATA_TYPE.put("UV", "Character Varying Unicode");
		DATA_TYPE.put("YI", "Year Interval (family code)");
		DATA_TYPE.put("YM", "Year-Month");
		DATA_TYPE.put("YR", "Year");
	}
	
	
	// 获取表/视图的SQL
	public final String COLUMNSET_SQL = "SELECT UPPER(TableName) TableName, TableKind ObjType, "
			+ "RequestText DDL, RequestTxtOverFlow, CommentString " 
			+ " from DBC.Tables "
			+ " where TableName <> '' AND TableKind IN (#TABLEKIND#) AND DatabaseName = ? "
			+ " order by CreateTimeStamp ASC";

	// 获取字段的SQL
	public final String COLUMN_SQL = "select UPPER(c.TableName) AS TABLE_NAME, UPPER(c.ColumnName) AS COLUMN_NAME, "
			+ "trim(c.ColumnType) AS TYPE_NAME, "
			+ "c.ColumnLength AS COLUMN_SIZE, 10 AS NUM_PREC_RADIX, c.Nullable AS IS_NULLABLE, "
			+ "c.DefaultValue AS COLUMN_DEF, 0 AS SQL_DATA_TYPE, "
			+ "0 AS SQL_DATETIME_SUB, c.ColumnId-1024 AS ORDINAL_POSITION, "
			+ "c.ColumnTitle AS COLUMN_TITLE, c.COMMENTSTRING, c.COMPRESSIBLE, "
			+ "c.DECIMALTOTALDIGITS, c.DECIMALFRACTIONALDIGITS, c.COLUMNFORMAT "
			+ " from DBC.COLUMNS c, DBC.TABLES t " 
			+ "where t.databasename=c.databasename and t.tablename=c.tablename "
			+ "and c.DatabaseName = ? and t.tablekind IN (#TABLEKIND#)";

	// 根据SCHEMA,表名，获取该表的DDL(DDL超过12500的长度时使用)
	private final String DDL_SQL = "SELECT TableName,TableKind, LineNo, RequestText FROM "
			+ "DBC.TableText WHERE DatabaseName = ? AND TableName=? ORDER BY LineNo";

	/**
	 * 结果集到Table的映射转换。
	 * 
	 */
	public class TableRowMapper implements RowMapper {
		private JdbcTemplate jdbcTemplate;

		private String schemaName;

		public TableRowMapper(JdbcTemplate jdbcTemplate, String schemaName)
		{
			this.jdbcTemplate = jdbcTemplate;
			this.schemaName = schemaName;
		}

		public Object mapRow(ResultSet resultSet, int i) throws SQLException {
			String tableName = resultSet.getString("tableName").trim();
			String ddl = null;
			// RequestTxtOverFlow的值为R,则表示该DDL超过12500，需要从dbc.tabletext表获取完整的DDL
			if ("R".equals(resultSet.getString("RequestTxtOverFlow"))) {
				List<?> list = jdbcTemplate.query(DDL_SQL, new Object[] { schemaName, tableName},
						new DDLRowMapper());
				StringBuilder sb = new StringBuilder();
				for(int j = 0; j < list.size(); j++){
					sb.append(list.get(j)).append("\r\n");
				}
				ddl = sb.toString();
			}
			if (resultSet.getString("ObjType").toString().equals("T")) {
				Table table = new Table();
				table.setName(tableName);
				table.setType(NamedColumnSetType.TABLE);
				if (ddl == null) {
					table.addAttr("ddl", resultSet.getString("DDL"));
				}
				else {
					table.addAttr("ddl", ddl);
				}
				table.addAttr("desc", resultSet.getString("CommentString"));
				table.addAttr(ModelElement.REMARKS, resultSet.getString("CommentString"));
				table.addAttr("objType", resultSet.getString("ObjType"));
				return table;
			}
			if (resultSet.getString("ObjType").toString().equals("V")) {
				View view = new View();
				view.setName(tableName);
				view.setType(NamedColumnSetType.VIEW);
				if (ddl == null) {
					view.addAttr("ddl", resultSet.getString("DDL"));
				}
				else {
					view.addAttr("ddl", ddl);
				}
				view.addAttr("desc", resultSet.getString("CommentString"));
				view.addAttr(ModelElement.REMARKS, resultSet.getString("CommentString"));
				view.addAttr("objType", resultSet.getString("ObjType"));
				return view;
			}
			else {
				TdMacro macro = new TdMacro();
				macro.setName(tableName);
				macro.addAttr(ModelElement.REMARKS, resultSet.getString("CommentString"));
				if (ddl == null) {
					macro.addAttr("ddl", resultSet.getString("DDL"));
				}
				else {
					macro.addAttr("ddl", ddl);
				}
				return macro;
			}
		}
	}

	public class DDLRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			return rs.getString("RequestText");
		}

	}

	public class ProcedureRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			Procedure procedure = new Procedure();
			procedure.setName(rs.getString("TableName"));
			procedure.addAttr(ModelElement.REMARKS, rs.getString("CommentString"));
			// int type = rs.getInt(Procedure.procedure_type);
			// procedure.addAttr(Procedure.procedure_type, type == 1 ?
			// "PROCEDURE" : (type == 2 ? "FUNCTION"
			// : "OTHERS"));
			return procedure;
		}

	}

	public class IndexRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			SQLIndex sIndex = new SQLIndex();
			// TDREADATA下的索引名称可能为null
			String indexName = rs.getString(SQLIndex.INDEX_NAME);
			// if (indexName == null || indexName.trim().equals("")) {
			indexName = new StringBuilder("INDEX.").append(rs.getString(Table.TABLE_NAME).trim()).append(".").append(
					rs.getString(Column.COLUMN_NAME).trim()).append(String.valueOf(rs.getInt("IndexNumber")))
					.toString();
			// }
			// else {//indexName 不同表字段的索引名称相同，导致元数据代码重复
			// indexName = indexName.trim();
			// }
			sIndex.setName(indexName);
			sIndex.setTableName(rs.getString(Table.TABLE_NAME).trim());
			sIndex.setColumnName(rs.getString(Column.COLUMN_NAME).trim());

			// 此处其实为唯一是否的标识,勿搞错
			String unique = rs.getString(SQLIndex.NON_UNIQUE).trim();
			if ("Y".equals(unique)) {
				sIndex.addAttr(SQLIndex.NON_UNIQUE, "N");
			}
			else {
				sIndex.addAttr(SQLIndex.NON_UNIQUE, "Y");
			}
			sIndex.addAttr(SQLIndex.INDEX_TYPE, rs.getString(SQLIndex.INDEX_TYPE));
			sIndex.addAttr(SQLIndex.ORDINAL_POSITION, String.valueOf(rs.getInt(SQLIndex.ORDINAL_POSITION)));
			// sIndex.addAttr(SQLIndex.CARDINALITY,
			// String.valueOf(rs.getInt(SQLIndex.CARDINALITY)));
			// sIndex.addAttr(SQLIndex.PAGES,
			// String.valueOf(rs.getInt(SQLIndex.PAGES)));

			return sIndex;
		}
	}

	/**
	 * 读取字段的字典信息
	 * @author fbchen
	 * @version 2.2 2011-07-05
	 */
	public static abstract class AbstractColumnRowCallbackHandler {
		
		/**
		 * 读取字段的字典信息
		 * @param rs 结果集
		 * @return Column对象
		 * @throws SQLException 获取SQL数据失败
		 */
		public Column readRow(ResultSet rs) throws SQLException {
			Column column = new Column();
			column.setName(rs.getString(Column.COLUMN_NAME).trim());
			//column.setDataType(rs.getInt(Column.DATA_TYPE));
			String typeName = rs.getString(Column.TYPE_NAME);
			column.setTypeName(DATA_TYPE.containsKey(typeName) ? DATA_TYPE.get(typeName) : "UNKNOWN");
			column.setColumnDef(rs.getString(Column.COLUMN_DEF));
			int length = rs.getInt(Column.COLUMN_SIZE);
			String precision = rs.getString("DECIMALTOTALDIGITS");
			String scale = rs.getString("DECIMALFRACTIONALDIGITS");
			column.setColumnSize(precision != null ? new Integer(precision) : length);
			column.setCharOctetLength(precision != null ? new Integer(precision) : null);
			column.setDecimalDigits(scale != null ? new Integer(scale) : null);
			// column.setBufferLength(rs.getInt(Column.BUFFER_LENGTH));
			column.setNumPrecRadix(rs.getInt(Column.NUM_PREC_RADIX));
			column.setSqlDataType(rs.getInt(Column.SQL_DATA_TYPE));
			column.setSqlDatetimeSub(rs.getInt(Column.SQL_DATETIME_SUB));
			column.setOrdinalPosition(rs.getInt(Column.ORDINAL_POSITION));
			column.setNullable("Y".equals(rs.getString(Column.IS_NULLABLE)));// 可为空
			column.setFormat(rs.getString("COLUMNFORMAT"));
			column.setCharacterSetName(null);
			column.setCompressible("Y".equals(rs.getString("Compressible")) ? true : null);
			column.addAttr(ModelElement.REMARKS, rs.getString("COLUMN_TITLE") != null ?
					rs.getString("COLUMN_TITLE") : rs.getString("CommentString"));
			return column;
		}
	}
	
	public class ColumnRowCallbackHandler extends AbstractColumnRowCallbackHandler implements RowCallbackHandler {

		private Map<String, ? extends NamedColumnSet> tableCache;

		public ColumnRowCallbackHandler(Map<String, ? extends NamedColumnSet> tableCache) {
			this.tableCache = tableCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String tableName = rs.getString(Table.TABLE_NAME).trim();
			if (!tableCache.containsKey(tableName)) {
				return;
			}
			Column column = this.readRow(rs);
			((NamedColumnSet) tableCache.get(tableName)).addColumn(column);
		}

	}

	public class AllColumnRowCallbackHandler extends AbstractColumnRowCallbackHandler implements RowCallbackHandler {
		private String schemaName;

		private Map<String, NamedColumnSet> columnSetCache;

		public AllColumnRowCallbackHandler(String schemaName,
				Map<String, NamedColumnSet> createColumnSetCache) {
			this.schemaName = schemaName;
			this.columnSetCache = createColumnSetCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String tableName = rs.getString(Table.TABLE_NAME).trim();
			String key = schemaName.concat(".").concat(tableName);
			if (!columnSetCache.containsKey(key)) { return; }

			Column column = this.readRow(rs);
			columnSetCache.get(key).addColumn(column);
		}
	}

}
