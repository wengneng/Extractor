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

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Partition;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.PrimaryKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;

/**
 * 实现公共DB元数据采集功能(JDBC方式+SQL方式)
 * 
 * @author chenhanqing
 * @version 1.0 Date: Jul 23, 2009
 */
public class OracleExtractServiceImpl extends DBExtractBaseServiceImpl {

	private final static List<String> systemTableList = new ArrayList<String>(12);
	{
		systemTableList.add("DBA_VIEWS");
		systemTableList.add("DBA_TAB_PARTITIONS");
		systemTableList.add("DBA_TABLES");
		systemTableList.add("DBA_TAB_COMMENTS");

		systemTableList.add("DBA_TAB_COLUMNS");
		systemTableList.add("DBA_COL_COMMENTS");
		systemTableList.add("DBA_CONS_COLUMNS");
		systemTableList.add("DBA_CONSTRAINTS");

		systemTableList.add("DBA_IND_COLUMNS");
		systemTableList.add("DBA_SOURCE");
		systemTableList.add("DBA_OBJECTS");
		systemTableList.add("DBA_ARGUMENTS");
	}

	private Log log = LogFactory.getLog(OracleExtractServiceImpl.class);

	@Override
	protected String signOfNoPrivilege() {
		return "表或视图不存在";
	}

	@Override
	protected List<String> getSystemTableList() {
		return systemTableList;
	}

	@SuppressWarnings("unchecked")
	protected List<NamedColumnSet> getTables(String schName) throws SQLException {
		log.info("开始采集表信息...");
		String sql = "SELECT NULL AS table_cat, A.OWNER AS table_schem, A.TABLE_NAME,"
				+ " 'TABLE' AS table_type, A.TABLESPACE_NAME, T.comments AS REMARKS"
				+ " FROM DBA_TABLES A, DBA_TAB_COMMENTS T WHERE  A.TABLE_NAME NOT LIKE '%==$0' ESCAPE '/'"
				+ " AND A.TABLE_NAME = T.table_name  AND A.OWNER = T.owner AND A.OWNER = ?";
		return super.getJdbcTemplate().query(sql, new Object[] { schName }, new TableRowMapper());
	}

	protected void setPartitions(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集表分区信息...");
		String sql = "SELECT P.TABLE_OWNER, P.TABLE_NAME, P.TABLESPACE_NAME, P.COMPOSITE, P.PARTITION_NAME, "
				+ "P.PARTITION_POSITION FROM ALL_TAB_PARTITIONS P WHERE P.TABLE_OWNER=?";

		super.getJdbcTemplate().query(sql, new Object[] { schName }, new PartitionRowCallbackHandler(tableCache));
	}

	private class PartitionRowCallbackHandler implements RowCallbackHandler {

		private Map<String, Table> tableCache;

		public PartitionRowCallbackHandler(Map<String, Table> tableCache)
		{
			this.tableCache = tableCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String tableName = rs.getString(Table.TABLE_NAME);
			if (tableCache.containsKey(tableName)) {
				Partition partition = new Partition();
				partition.setName(rs.getString(Partition.PARTITION_NAME));
				partition.setTableName(tableName);
				partition.addAttr(Partition.COMPOSITE, rs.getString(Partition.COMPOSITE));
				partition
						.addAttr(Partition.PARTITION_POSITION, String.valueOf(rs.getInt(Partition.PARTITION_POSITION)));
				partition.addAttr(Partition.TABLESPACE_NAME, rs.getString(Partition.TABLESPACE_NAME));

				tableCache.get(tableName).addPartition(partition);
			}

		}

	}

	/**
	 * 设置视图的SQL属性
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl#setViewText(java.lang.String,
	 *      java.util.Map)
	 */
	protected void setViewText(String schName, Map<String, View> viewCache) throws SQLException {
		String sql = "SELECT VIEW_NAME, TEXT AS sql FROM DBA_VIEWS WHERE OWNER = ?";

		super.getJdbcTemplate().query(sql, new Object[] { schName }, new ViewRowCallbackHandler(viewCache));
	}

	private class ViewRowCallbackHandler implements RowCallbackHandler {
		private Map<String, View> viewCache;

		public ViewRowCallbackHandler(Map<String, View> viewCache)
		{
			this.viewCache = viewCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String viewText = rs.getString("VIEW_NAME");
			if (viewCache.containsKey(viewText)) {
				viewCache.get(viewText).addAttr(View.SQL, rs.getString(View.SQL));
			}

		}

	}

	private class TableRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			Table table = new Table();
			table.setName(rs.getString(Table.TABLE_NAME));
			// table.addAttr(Table.TABLE_TYPE, rs.getString(Table.TABLE_TYPE));

			table.addAttr(Table.TABLESPACE_NAME, rs.getString(Table.TABLESPACE_NAME));
			table.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));

			table.setType(NamedColumnSetType.TABLE);

			return table;
		}

	}

	protected void setTableColumns(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集表的字段信息...");
		String sql = "SELECT NULL AS table_cat, t.owner AS table_schem,"
				+ " t.table_name AS table_name,t.column_name AS column_name, DECODE(t.data_type,'CHAR',"
				+ " 1, 'VARCHAR2',12, 'NUMBER', 3,'LONG', -1, 'DATE', 91,'RAW', -3, 'LONG RAW', -4, 'BLOB',"
				+ " 2004, 'CLOB', 2005, 'BFILE', -13, 'FLOAT', 6, 'TIMESTAMP(6)', 93, 'TIMESTAMP(6) WITH TIME ZONE',"
				+ " -101, 'TIMESTAMP(6) WITH LOCAL TIME ZONE', -102, 'INTERVAL YEAR(2) TO MONTH', -103,"
				+ " 'INTERVAL DAY(2) TO SECOND(6)', -104, 'BINARY_FLOAT', 100, 'BINARY_DOUBLE', 101,"
				+ " 1111) AS data_type, t.data_type AS type_name,"
				+ " DECODE(t.data_precision, null, t.data_length, t.data_precision) AS column_size,"
				+ " 0 AS buffer_length, t.data_scale AS decimal_digits, 10 AS num_prec_radix,"
				+ " DECODE(t.nullable, 'N', 0, 1) AS nullable, d.comments AS remarks, t.data_default AS column_def,"
				+ " 0 AS sql_data_type, 0 AS sql_datetime_sub, t.data_length AS char_octet_length,"
				+ " t.column_id AS ordinal_position, DECODE(t.nullable, 'N', 'NO', 'YES') AS is_nullable"
				+ " FROM dba_tab_columns t, DBA_COL_COMMENTS d  WHERE  t.COLUMN_NAME=d.column_name"
				+ " AND t.TABLE_NAME = d.table_name  AND t.OWNER = d.owner AND t.owner =? AND t.table_name NOT LIKE '%==$0' ";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new ColumnRowCallbackHandler(tableCache));
	}

	private class ColumnRowCallbackHandler implements RowCallbackHandler {

		private Map<String, Table> tableCache;

		public ColumnRowCallbackHandler(Map<String, Table> tableCache)
		{
			this.tableCache = tableCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String tableName = rs.getString(Table.TABLE_NAME);
			if (!tableCache.containsKey(tableName)) { return; }

			String colName = rs.getString(Column.COLUMN_NAME);
			Column column = new Column();
			column.setName(colName);
			String typeName = rs.getString(Column.TYPE_NAME);
			if (typeName != null && typeName.indexOf("CHAR") >= 0) {
				typeName = typeName.substring(0, typeName.indexOf("CHAR") + "CHAR".length());
			}
			// column.addAttr(Column.TYPE_NAME, typeName);
			// column.addAttr(Column.COLUMN_DEF,
			// rs.getString(Column.COLUMN_DEF));
			column.setTypeName(typeName);
			column.setColumnDef(rs.getString(Column.COLUMN_DEF));
			column.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
			column.setDataType(rs.getInt(Column.DATA_TYPE));
			column.setColumnSize(rs.getInt(Column.COLUMN_SIZE));
			column.setBufferLength(rs.getInt(Column.BUFFER_LENGTH));
			column.setDecimalDigits(rs.getInt(Column.DECIMAL_DIGITS));
			column.setNumPrecRadix(rs.getInt(Column.NUM_PREC_RADIX));
			column.setSqlDataType(rs.getInt(Column.SQL_DATA_TYPE));
			column.setSqlDatetimeSub(rs.getInt(Column.SQL_DATETIME_SUB));
			column.setCharOctetLength(rs.getInt(Column.CHAR_OCTET_LENGTH));
			column.setOrdinalPosition(rs.getInt(Column.ORDINAL_POSITION));
			String nullable = rs.getString(Column.IS_NULLABLE);
			if ("YES".equals(nullable)) {
				// 可为空
				column.setNullable(true);
			}

			Table table = tableCache.get(tableName);
			table.addColumn(column);

		}

	}

	/**
	 * 获取表的主键
	 * 
	 * @param schName
	 * @param tableCache
	 */
	protected void setPrimaryKeies(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集主键信息...");
		String sql = "SELECT NULL AS table_cat, c.owner AS table_schem,"
				+ "c.table_name, c.column_name, c.position AS key_seq, c.constraint_name AS pk_name "
				+ "FROM dba_cons_columns c, dba_constraints k WHERE "
				+ " k.constraint_name = c.constraint_name "
				+ " AND k.table_name = c.table_name   AND k.owner = c.owner AND k.owner= ?  AND k.constraint_type = 'P' ";

		super.getJdbcTemplate().query(sql, new Object[] { schName }, new PrimaryKeyRowCallbackHandler(tableCache));
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
				primaryKey.setName(rs.getString(PrimaryKey.PK_NAME));
				primaryKey.setColumnName(rs.getString(Column.COLUMN_NAME));
				primaryKey.addAttr(PrimaryKey.KEY_SEQ, String.valueOf(rs.getInt(PrimaryKey.KEY_SEQ)));

				Table table = tableCache.get(tableName);
				table.addPrimaryKey(primaryKey);

			}

		}

	}

	@SuppressWarnings("unchecked")
	protected List<SQLIndex> getIndexs(String schName) throws SQLException {
		log.info("开始采集索引信息...");
		String sql = "select null as table_cat, i.owner as table_schem, i.table_name,"
				+ "decode(i.uniqueness, 'UNIQUE', 0, 1) as NON_UNIQUE, null as index_qualifier,"
				+ " i.index_name,  1 as type,  c.column_position as ordinal_position,  c.column_name,"
				+ " null as asc_or_desc,  i.distinct_keys as cardinality,  i.leaf_blocks as pages,"
				+ " null as filter_condition  from dba_indexes i, dba_ind_columns c"
				+ " where  i.index_name = c.index_name  and i.table_owner = c.table_owner"
				+ " and i.table_name = c.table_name  and i.owner = c.index_owner AND  i.owner = ?";
		return super.getJdbcTemplate().query(sql, new Object[] { schName }, new IndexRowMapper());
	}

	private class IndexRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			SQLIndex sIndex = new SQLIndex();
			sIndex.setName(rs.getString(SQLIndex.INDEX_NAME));
			sIndex.setTableName(rs.getString(Table.TABLE_NAME));
			sIndex.setColumnName(rs.getString(Column.COLUMN_NAME));

			int nonUnique = rs.getInt(SQLIndex.NON_UNIQUE);
			if (nonUnique == 0) {
				sIndex.addAttr(SQLIndex.NON_UNIQUE, "UNIQUE");
			}
			else {
				sIndex.addAttr(SQLIndex.NON_UNIQUE, "NON_UNIQUE");
			}
			sIndex.addAttr(SQLIndex.ORDINAL_POSITION, String.valueOf(rs.getInt(SQLIndex.ORDINAL_POSITION)));
			sIndex.addAttr(SQLIndex.CARDINALITY, String.valueOf(rs.getInt(SQLIndex.CARDINALITY)));
			sIndex.addAttr(SQLIndex.PAGES, String.valueOf(rs.getInt(SQLIndex.PAGES)));

			return sIndex;
		}

	}

	@SuppressWarnings("unchecked")
	protected List<NamedColumnSet> getViews(String schName) throws SQLException {
		log.info("开始采集ORALCE视图信息...");
		// 从DBA*表采集视图
		String sql = "SELECT NULL AS table_cat, " + "o.owner AS table_schem,o.object_name AS table_name, "
				+ "o.object_type AS table_type,NULL AS remarks "
				+ "FROM dba_objects o WHERE o.owner LIKE ? ESCAPE '/' " + "AND o.object_type ='VIEW'";
		return super.getJdbcTemplate().query(sql, new Object[] { schName }, new ViewRowMapper());
	}

	private class ViewRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			View view = new View();
			view.setName(rs.getString(Table.TABLE_NAME));
			// table.addAttr(Table.TABLE_TYPE, rs.getString(Table.TABLE_TYPE));

			// table.addAttr(Table.TABLESPACE_NAME,
			// rs.getString(Table.TABLESPACE_NAME));
			view.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));

			view.setType(NamedColumnSetType.VIEW);

			return view;
		}

	}

	protected void setProceduresText(String schName, Map<String, Procedure> procedureCache) throws SQLException {

		String sql = "SELECT S.name AS PROCEDURE_NAME, S.TEXT FROM DBA_SOURCE S WHERE S.OWNER=? ORDER BY S.name, S.line ASC";
		super.getJdbcTemplate().query(sql, new Object[] { schName }, new ProcedureRowCallbackHandler(procedureCache));
	}

	private class ProcedureRowCallbackHandler implements RowCallbackHandler {

		private Map<String, Procedure> procedureCache;

		public ProcedureRowCallbackHandler(Map<String, Procedure> procedureCache)
		{
			this.procedureCache = procedureCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String procName = rs.getString(Procedure.PROCEDURE_NAME);
			if (procedureCache.containsKey(procName)) {
				// 找到存储过程的内容
				procedureCache.get(procName).setText(rs.getString(Procedure.TEXT));
			}

		}

	}

	/**
	 * 建立视图与视图间,视图与表间的依赖关系
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl.DBExtractBaseServiceImpl#setViewDependency(java.lang.String,
	 *      java.util.Map)
	 */
	protected void setViewDependency(String schName, Map<String, View> viewCache) throws SQLException {
		String sql = "SELECT A.REFERENCED_OWNER AS SCH_NAME, A.REFERENCED_NAME AS TABLE_NAME, A.name, A.REFERENCED_TYPE"
				+ " FROM SYS.DBA_DEPENDENCIES A  WHERE "
				+ " A.REFERENCED_TYPE IN ( 'TABLE', 'VIEW') AND A.TYPE = 'VIEW'  AND A.OWNER = ?";

		super.getJdbcTemplate().query(sql, new Object[] { schName }, new ViewReferenceRowCallbackHandler(viewCache));
	}

	private class ViewReferenceRowCallbackHandler implements RowCallbackHandler {

		private Map<String, View> viewCache;

		public ViewReferenceRowCallbackHandler(Map<String, View> viewCache)
		{
			this.viewCache = viewCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String viewName = rs.getString("NAME");
			if (viewCache.containsKey(viewName)) {
				// 包含于视图缓存中
				String tabName = rs.getString(Table.TABLE_NAME);
				String schName = rs.getString(Schema.SCH_NAME);
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
