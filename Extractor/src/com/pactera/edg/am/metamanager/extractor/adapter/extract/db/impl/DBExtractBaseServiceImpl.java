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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.IExtractFilter;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ForeignKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.PrimaryKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ProcedureColumn;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.TdMacro;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Trigger;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;

/**
 * 实现公共DB元数据采集功能(JDBC方式)
 * 
 * @author user
 * @version 1.0 Date: Jul 23, 2009
 * 
 */
public class DBExtractBaseServiceImpl extends AbstractDBExtractService {

	private Log log = LogFactory.getLog(DBExtractBaseServiceImpl.class);

	protected void setTableColumns(String schName, Map<String, Table> tableCache) throws SQLException {
		ResultSet rs = null;
		try {
			log.info("开始采集表的字段信息...");
			rs = super.getMetadata().getColumns(null, schName, null, null);
			while (rs.next()) {
				String tableName = rs.getString(Table.TABLE_NAME);
				if (!tableCache.containsKey(tableName)) {
					continue;
				}

				Column column = genColumn(rs);
				Table table = tableCache.get(tableName);
				table.addColumn(column);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}
	}

	private Column genColumn(ResultSet rs) throws SQLException {
		Column column = new Column();
		String colName = rs.getString(Column.COLUMN_NAME);
		column.setName(colName);
		String typeName = rs.getString(Column.TYPE_NAME);
		if (typeName != null && typeName.indexOf("CHAR") >= 0) {
			typeName = typeName.substring(0, typeName.indexOf("CHAR") + "CHAR".length());
		}
		// column.addAttr(Column.TYPE_NAME, typeName);
		column.setTypeName(typeName);
		column.setColumnDef(rs.getString(Column.COLUMN_DEF)); // 默认值
		// column.addAttr(Column.COLUMN_DEF, rs.getString(Column.COLUMN_DEF));
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

		return column;
	}

	/**
	 * 是否我们需要采集的Schema
	 * 
	 * @param schName
	 *            Schema Name
	 * @return
	 */
	protected boolean isMatchExtractSchema(String schName) {
		String[] extractSchemas = super.getExtractSchemas().toUpperCase().split("\\s*,\\s*");
		Arrays.sort(extractSchemas);

		return Arrays.binarySearch(extractSchemas, schName.toUpperCase()) >= 0;
	}

	protected List<Schema> getSchemas() throws SQLException {
		List<Schema> schemas = new ArrayList<Schema>();
		log.info("开始采集Schema信息...");
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getSchemas();
			while (rs.next()) {
				String schName = rs.getString("TABLE_SCHEM");
				if (isMatchExtractSchema(schName)) {
					Schema schema = new Schema();
					String dataType = super.getMetadata().getDatabaseProductName();
					if(dataType.equals("PostgreSQL")){
						schName = schName.toLowerCase();
					}else{
						schName = schName.toUpperCase();
					}
					schema.setName(schName);
					schemas.add(schema);
				}
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}
		return schemas;
	}

	protected List<TdMacro> getMacro(String schName) {
		return Collections.emptyList();
	}

	/**
	 * 该接口实现真是一般般,只能取一个表的是或不是唯一的索引
	 * 
	 * @param schName
	 * @return
	 * @throws SQLException
	 */
	protected List<SQLIndex> getIndexs(String schName) throws SQLException {
		log.info("开始采集索引信息...");
		return Collections.emptyList();
	}

	protected List<NamedColumnSet> getTables(String schName) throws SQLException {
		List<NamedColumnSet> tables = new ArrayList<NamedColumnSet>();
		log.info("开始采集表信息...");
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getTables(null, schName, null, new String[] { "TABLE" });
			while (rs.next()) {
				String tabName = rs.getString(Table.TABLE_NAME);
				if (tabName.matches(".*==\\$0$")) {
					// TODO 回头记得改改这里 !!加在metaData.getTables处来过滤
					continue;
				}

				Table table = new Table();
				table.setName(tabName);
				// table.addAttr(Table.TABLE_TYPE,
				// rs.getString(Table.TABLE_TYPE));
				table.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
				table.setType(NamedColumnSetType.TABLE);
				tables.add(table);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}
		return tables;
	}

	protected void setForeignKeies(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集外键信息...");
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getImportedKeys(null, schName, null);
			while (rs.next()) {
				String tableName = rs.getString(ForeignKey.FKTABLE_NAME);
				if (!tableCache.containsKey(tableName)) {
					continue;
				}

				ForeignKey fk = new ForeignKey();
				String name = rs.getString(ForeignKey.FK_NAME);
				if (name == null || name.equals("")) {
					name = new StringBuilder("FK.").append(rs.getString(ForeignKey.FKCOLUMN_NAME)).append("_").append(
							rs.getString(ForeignKey.PKCOLUMN_NAME)).toString();
				}
				fk.setName(name);
				fk.setFKColumnName(rs.getString(ForeignKey.FKCOLUMN_NAME));

				fk.setPKColumnName(rs.getString(ForeignKey.PKCOLUMN_NAME));
				fk.setPKTableName(rs.getString(ForeignKey.PKTABLE_NAME));
				fk.setPKSchemaName(rs.getString(ForeignKey.PKTABLE_SCHEM));

				fk.addAttr(ForeignKey.KEY_SEQ, String.valueOf(rs.getInt(ForeignKey.KEY_SEQ)));
				Table table = tableCache.get(tableName);
				table.addForeignKey(fk);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}
	}

	/**
	 * 查询表的分区
	 * 
	 * @param schName
	 *            schema
	 * @param tableCache
	 *            表
	 * @throws SQLException
	 *             SQL-Exception
	 */
	protected void setPartitions(String schName, Map<String, Table> tableCache) throws SQLException {

	}

	/**
	 * 查询表的约束
	 * 
	 * @param schName
	 *            schema
	 * @param tableCache
	 *            表
	 * @throws SQLException
	 *             SQL-Exception
	 */
	protected void setConstraints(String schName, Map<String, Table> tableCache) throws SQLException {

	}

	/**
	 * 该拿不到所有主键的信息,而只能一个个表的主键获取,SHIT
	 * 
	 * @param schName
	 * @param tableCache
	 * @throws SQLException
	 */
	protected void setPrimaryKeies(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集主键信息...");
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getPrimaryKeys(null, schName, null);
			while (rs.next()) {
				String tableName = rs.getString(Table.TABLE_NAME);
				if (!tableCache.containsKey(tableName)) {
					continue;
				}

				// TD中存在主键名称为null的情况,需作兼容处理
				String pkName = rs.getString(PrimaryKey.PK_NAME);
				if (null == pkName || "".equals(pkName)) {
					pkName = new StringBuilder("PK.").append(tableName).append(".").append(
							String.valueOf(rs.getInt(PrimaryKey.KEY_SEQ))).toString();
				}
				PrimaryKey pk = new PrimaryKey();
				pk.setName(pkName);
				pk.setColumnName(rs.getString(Column.COLUMN_NAME));
				pk.addAttr(PrimaryKey.KEY_SEQ, String.valueOf(rs.getInt(PrimaryKey.KEY_SEQ)));

				// pk.setName(PrimaryKey.PK_NAME);
				Table table = tableCache.get(tableName);
				table.addPrimaryKey(pk);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}
	}

	protected List<Procedure> getProcedures(String schName) throws SQLException {
		log.info("开始采集存储过程信息...");
		List<Procedure> procedures = new ArrayList<Procedure>();
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getProcedures("", schName, null);
			while (rs.next()) {
				Procedure procedure = new Procedure();
				procedure.setName(rs.getString(Procedure.PROCEDURE_NAME));
				procedure.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
				int type = rs.getInt(Procedure.PROCEDURE_TYPE);
				procedure.addAttr(Procedure.PROCEDURE_TYPE, type == 1 ? "PROCEDURE" : (type == 2 ? "FUNCTION"
						: "OTHERS"));
				procedures.add(procedure);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}
		return procedures;
	}

	protected void setProceduresText(String schName, Map<String, Procedure> procedureCache) throws SQLException {

	}

	protected void setProcedureColumns(String schName, Map<String, Procedure> procedureCache) throws SQLException {
		log.info("开始采集存储过程参数信息...");
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getProcedureColumns("", schName, null, null);
			while (rs.next()) {
				String procName = rs.getString(Procedure.PROCEDURE_NAME);
				if (!procedureCache.containsKey(procName)) {
					continue;
				}
				String pColumnName = rs.getString(ProcedureColumn.COLUMN_NAME);
				if (null == pColumnName || pColumnName.equals("")) {
					// 返回数据类型,没有名称
					continue;
				}

				ProcedureColumn pColumn = new ProcedureColumn();
				pColumn.setName(pColumnName);

				pColumn.addAttr(ProcedureColumn.COLUMN_TYPE, String.valueOf(rs.getInt(ProcedureColumn.COLUMN_TYPE)));
				pColumn.addAttr(ProcedureColumn.DATA_TYPE, String.valueOf(rs.getInt(ProcedureColumn.DATA_TYPE)));
				pColumn.addAttr(ProcedureColumn.TYPE_NAME, rs.getString(ProcedureColumn.TYPE_NAME));
				pColumn.addAttr(ProcedureColumn.PRECISION, String.valueOf(rs.getInt(ProcedureColumn.PRECISION)));

				pColumn.addAttr(ProcedureColumn.LENGTH, String.valueOf(rs.getInt(ProcedureColumn.LENGTH)));
				pColumn.addAttr(ProcedureColumn.SCALE, String.valueOf(rs.getInt(ProcedureColumn.SCALE)));
				pColumn.addAttr(ProcedureColumn.RADIX, String.valueOf(rs.getInt(ProcedureColumn.RADIX)));
				pColumn.addAttr(ProcedureColumn.NULLABLE, "TRUE");// String.valueOf(rs.getInt(ProcedureColumn.NULLABLE)));

				pColumn.addAttr(ModelElement.REMARKS, rs.getString(ModelElement.REMARKS));
				try {
					pColumn.addAttr(ProcedureColumn.SEQUENCE, String.valueOf(rs.getInt(ProcedureColumn.SEQUENCE)));
					pColumn.addAttr(ProcedureColumn.OVERLOAD, rs.getString(ProcedureColumn.OVERLOAD));
					pColumn.addAttr(ProcedureColumn.DEFAULT_VALUE, rs.getString(ProcedureColumn.DEFAULT_VALUE));
				}
				catch (SQLException s) {
					if (log.isDebugEnabled()) {
						log.warn("存储过程字段不存在SEQUENCE,OVERLOAD,DEFAULT_VALUE的属性!");
					}
				}

				procedureCache.get(procName).addPColumn(pColumn);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}

	}

	protected List<NamedColumnSet> getViews(String schName) throws SQLException {
		log.info("开始采集视图信息...");
		List<NamedColumnSet> namedColumnSets = new ArrayList<NamedColumnSet>();
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getTables(null, schName, null, new String[] { "VIEW" });

			while (rs.next()) {
				String tabName = rs.getString(Table.TABLE_NAME);
				View view = new View();
				view.setName(tabName);
				view.setType(NamedColumnSetType.VIEW);
				namedColumnSets.add(view);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}
		return namedColumnSets;
	}

	protected void setViewText(String schName, Map<String, View> viewCache) throws SQLException {

	}

	protected void setViewDependency(String schName, Map<String, View> viewCache) throws SQLException {
		log.info("开始采集视图与表的依赖信息...");
	}

	protected void setViewColumns(String schName, Map<String, View> viewCache) throws SQLException {
		log.info("开始采集视图字段信息...");
		ResultSet rs = null;
		try {
			rs = super.getMetadata().getColumns(null, schName, null, null);
			while (rs.next()) {
				String viewName = rs.getString(Table.TABLE_NAME);
				String dataType = super.getMetadata().getDatabaseProductName();
				if(dataType.equals("PostgreSQL")){
					viewName = viewName.toLowerCase();
				}else{
					viewName = viewName.toUpperCase();
				}
				if (!viewCache.containsKey(viewName)) {
					continue;
				}

				Column column = genColumn(rs);

				View view = viewCache.get(viewName);
				view.addColumn(column);
			}
		}
		finally {
			if (rs != null) {
				rs.close();
			}
		}

	}

	protected List<Trigger> getTriggers(String schemaName) throws SQLException {
		return Collections.emptyList();
	}

	@Override
	protected List<String> getSystemTableList() {
		return Collections.emptyList();
	}

	@Override
	protected String signOfNoPrivilege() {
		return "";
	}

	@Override
	protected IExtractFilter getExtractFilter() {
		return new DBExtractFilterImpl();
	}

}
