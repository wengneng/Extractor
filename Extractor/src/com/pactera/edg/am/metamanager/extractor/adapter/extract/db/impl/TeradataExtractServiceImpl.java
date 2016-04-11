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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.cxf.common.util.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.db.IDBExtractService;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.db.helper.TeradataExtractHelper;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Catalog;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.TdMacro;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.IDBDictionaryDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Log4jInit;
import com.pactera.edg.am.sqlparser.SQLParserLoader;

/**
 * Teradata数据库的数据字典采集器
 * 
 * @author chenhanqing
 * @version 1.0 Date: Sep 9, 2010
 */
public class TeradataExtractServiceImpl extends DBExtractBaseServiceImpl {

	private Log log = LogFactory.getLog(TeradataExtractServiceImpl.class);

	private static TeradataExtractHelper helper = new TeradataExtractHelper();

	private Schema cntSchema;

	private Catalog catalog;

	private Map<String, NamedColumnSetType> types;

	private List<Schema> schemaList = new ArrayList<Schema>();

	/**
	 * 缓存在解析视图与表间的依赖关系时，需要创建的表/视图的信息
	 */
	private Map<String, NamedColumnSet> createColumnSetCache = new HashMap<String, NamedColumnSet>();

	@SuppressWarnings("unchecked")
	protected List<SQLIndex> getIndexs(String schName) throws SQLException {
		log.info("开始采集索引信息...");
		String sql = "select  UPPER(TableName) as table_name,"
				+ " UniqueFlag as NON_UNIQUE, null as index_qualifier,"
				+ " IndexName as index_name, ColumnPosition as ordinal_position, UPPER(ColumnName) as column_name,"
				+ " null as asc_or_desc,  AccessCount as cardinality,  AccessCount as pages,"
				+ " null as filter_condition, IndexType as INDEX_TYPE, IndexNumber from dbc.indices where databasename= ? ";
		return super.getJdbcTemplate().query(sql, new Object[] { schName }, helper.new IndexRowMapper());
	}

	protected void setTableColumns(String schName, Map<String, Table> tableCache) throws SQLException {
		log.info("开始采集字段信息...");
		super.getJdbcTemplate().query(
				helper.COLUMN_SQL.replace("#TABLEKIND#", "'T'"),
				new Object[] { schName },
				helper.new ColumnRowCallbackHandler(tableCache));
	}

	protected void setViewColumns(String schName, Map<String, View> viewCache) throws SQLException {
		log.info("开始采集视图字段信息 ...");
		super.getJdbcTemplate().query(
				helper.COLUMN_SQL.replace("#TABLEKIND#", "'V'"),
				new Object[] { schName },
				helper.new ColumnRowCallbackHandler(viewCache));
	}

	@SuppressWarnings("unchecked")
	protected List<NamedColumnSet> getTables(String schName) throws SQLException {
		log.info("开始采集表信息...");

		return super.getJdbcTemplate().query(helper.COLUMNSET_SQL.replace("#TABLEKIND#", "'T'"),
				new Object[] { schName }, helper.new TableRowMapper(super.getJdbcTemplate(), schName));
	}

	@SuppressWarnings("unchecked")
	private List<NamedColumnSet> getTables(String schemaName, List<String> tableList) throws SQLException {
		List<NamedColumnSet> retrunTables = new ArrayList<NamedColumnSet>(1);
		boolean hasCache = false;
		for (String tableName : tableList) {
			String key = schemaName.concat(".").concat(tableName);
			if (createColumnSetCache.containsKey(key)) {
				// 缓存中已经存在该表/视图，直接从缓存中获取
				retrunTables.add(createColumnSetCache.get(key));
				hasCache = true;
			}
		}
		if (hasCache) {
			return retrunTables;
		}

		log.info("开始采集视图的依赖表/视图的信息,Schema:" + schemaName);
		// 缓存中不存在，则通过JDBC从库中获取
		List<NamedColumnSet> tables = super.getJdbcTemplate().query(
				helper.COLUMNSET_SQL.replace("#TABLEKIND#", "'T', 'V'"),
				new Object[] { schemaName },
				helper.new TableRowMapper(super.getJdbcTemplate(), schemaName));
		for (NamedColumnSet table : tables) {
			String key = schemaName.concat(".").concat(table.getName());
			createColumnSetCache.put(key, table);
		}
		log.info("开始采集视图的依赖表/视图字段的信息,Schema:" + schemaName);
		super.getJdbcTemplate().query(
				helper.COLUMN_SQL.replace("#TABLEKIND#", "'T', 'V'"),
				new Object[] { schemaName },
				helper.new AllColumnRowCallbackHandler(schemaName, createColumnSetCache));

		for (String tableName : tableList) {
			String key = schemaName.concat(".").concat(tableName);
			if (createColumnSetCache.containsKey(key)) {
				// 缓存中已经存在该表/视图，直接从缓存中获取
				retrunTables.add(createColumnSetCache.get(key));
			}
		}

		return retrunTables;
	}

	@SuppressWarnings("unchecked")
	protected List<NamedColumnSet> getViews(String schName) throws SQLException {
		log.info("开始采集视图信息...");
		return super.getJdbcTemplate().query(helper.COLUMNSET_SQL.replace("#TABLEKIND#", "'V'"),
				new Object[] { schName }, helper.new TableRowMapper(super.getJdbcTemplate(), schName));
	}

	@SuppressWarnings("unchecked")
	protected List<TdMacro> getMacro(String schName) {
		log.info("开始采集宏信息...");
		return super.getJdbcTemplate().query(helper.COLUMNSET_SQL.replace("#TABLEKIND#", "'M'"),
				new Object[] { schName }, helper.new TableRowMapper(super.getJdbcTemplate(), schName));

	}

	@SuppressWarnings("unchecked")
	protected List<Procedure> getProcedures(String schName) {
		log.info("开始采集存储过程信息...");
		return super.getJdbcTemplate().query(helper.COLUMNSET_SQL.replace("#TABLEKIND#", "'P'"),
				new Object[] { schName }, helper.new ProcedureRowMapper());

	}

	protected void afterHook(Catalog catalog) {
		setDependenciesBetweenViewAndTable(catalog);
		setDependenciesBetweenMacroAndTable(catalog);
	}

	private void setDependenciesBetweenMacroAndTable(Catalog catalog) {
		log.info("开始建立TD数据库宏与表之间的依赖关系...");
		this.catalog = catalog;
		types = new HashMap<String, NamedColumnSetType>();
		/**
		 * 解析加载，负责从数据库中访问已有的元数据
		 */
		SQLParserLoader loader = new SQLParserLoader();
		try {
			loader.setDbDictionaryDao(new TeradataDictionaryDaoImpl());
			List<Schema> schemas = this.catalog.getSchemas();
			for (Schema schema : schemas) {
				List<TdMacro> macros = schema.getMacros();

				// 把视图SQL集合起来,统一解析
				cntSchema = schema;
				List<HashMap<String, String>> tableRel = new ArrayList<HashMap<String, String>>();
				for (TdMacro macro : macros) {

					String sql = macro.getAttrs().get("ddl");
					HashMap hashMap = loader.parseMacroSQL(sql, schema.getName());
					if (hashMap != null && hashMap.get("TABLE_RELATIONS") != null) {
						tableRel.addAll((ArrayList<HashMap<String, String>>) hashMap.get("TABLE_RELATIONS"));
					}
					else {
						log.warn("解析宏失败！" + sql);
					}
					// 暂时注释
					// 补充测试数据
				}

				if (tableRel.size() == 0) {
					log
							.error(new StringBuilder("解析schema:").append(schema.getName()).append(
									"的宏与表的依赖关系时，宏解析器返回内容为空！ "));
					continue;
				}
				log.info("开始解析schema:" + schema.getName() + "的宏与表间的依赖关系...");
				setDependenciesMacroTable(tableRel);
				log.info("成功解析schema:" + schema.getName() + "的宏与表间的依赖关系!");
			}
			log.info("成功解析宏与表间的依赖关系!");
			// 增加关系元数据,由于在视图处已增加了部分Schema
			// 由于宏的处理排在视图的后面，所以取消在视图处给catalog增加schema，改为在处理宏的关系时再补上
			catalog.getSchemas().addAll(schemaList);
		}
		catch (Exception e) {
			log.error("解析宏与表间的依赖关系时发生异常", e);
			AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN, "解析宏与表间的依赖关系时发生异常:" + e.getMessage());

		}
		finally {
			// 擦擦屁股
			this.catalog = null;
			cntSchema = null;
			types = null;
			createColumnSetCache = null;
		}
	}

	@SuppressWarnings("unchecked")
	private void setDependenciesBetweenViewAndTable(Catalog catalog) {
		log.info("开始建立TD数据库视图与表之间的依赖关系...");
		this.catalog = catalog;
		types = new HashMap<String, NamedColumnSetType>();
		/**
		 * 解析加载，负责从数据库中访问已有的元数据
		 */
		SQLParserLoader loader = new SQLParserLoader();
		try {
			loader.setDbDictionaryDao(new TeradataDictionaryDaoImpl());
			List<Schema> schemas = this.catalog.getSchemas();
			for (Schema schema : schemas) {
				List<NamedColumnSet> columnSets = schema.getColumnSets();
				// 对表作进行排序
				Collections.sort(columnSets);
				List<String> sqlList = new ArrayList<String>(columnSets.size());
				for (NamedColumnSet columnSet : columnSets) {
					if (columnSet.getType() == NamedColumnSetType.VIEW) {
						// 创建视图的SQL
						String sql = columnSet.getAttrs().get("ddl");
						sqlList.add(sql);
					}

				}
				// 把视图SQL集合起来,统一解析
				cntSchema = schema;
				List<HashMap<String, String>> tableRel = new ArrayList<HashMap<String, String>>();
				List<HashMap<String, String>> columnRel = new ArrayList<HashMap<String, String>>();
				for (String sql : sqlList) {
					List<String> singleViewList = new ArrayList<String>();
					singleViewList.add(sql);
					HashMap hashMap = loader.parseSQLs(singleViewList, schema.getName());
					if (hashMap.get("TABLE_RELATIONS") != null && hashMap.get("COLUMN_RELATIONS") != null) {
						tableRel.addAll((ArrayList<HashMap<String, String>>) hashMap.get("TABLE_RELATIONS"));
						columnRel.addAll((ArrayList<HashMap<String, String>>) hashMap.get("COLUMN_RELATIONS"));
					}
					else {
						log.warn("解析SQL失败！" + sql);
					}
				}

				if (columnRel.size() == 0 && tableRel.size() == 0) {
					log.error(new StringBuilder("解析schema:").append(schema.getName()).append(
							"的视图与表/视图间的依赖关系时，SQL解析器返回内容为空！ "));
					continue;
				}
				log.info("开始解析schema:" + schema.getName() + "的视图与表/视图间的依赖关系...");
				setDependenciesTable(tableRel);
				setDependenciesBetweenViewAndTable(columnRel);
				log.info("成功解析schema:" + schema.getName() + "的视图与表/视图间的依赖关系!");
			}
			log.info("成功解析视图与视图/表间的依赖关系!");
			// 增加关系元数据
			// 由于处理宏在处理视图的后面，所以这里取消增加schema，改为在宏的处理过程里增加
			// catalog.getSchemas().addAll(schemaList);
		}
		catch (Exception e) {
			log.error("解析视图与视图/表间的依赖关系时发生异常", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.WARN, "解析视图与视图/表间的依赖关系时发生异常:" + e.getMessage());

		}
		finally {
			// 擦擦屁股
			this.catalog = null;
			cntSchema = null;
			types = null;
			// 暂时注释掉，uncle
			// createColumnSetCache = null;
		}
	}

	private void setDependenciesTable(List<HashMap<String, String>> columnRel) {
		for (HashMap<String, String> cols : columnRel) {
			// 转换成大写形式
			String srcObj = cols.get("Src_Obj").toUpperCase();
			String tgtObj = cols.get("Tgt_Obj").toUpperCase();
			// 分别找到SRC,TGT所在的位置,然后建立它们之间的关系
			// 注意:它们的长度都为3,即为schema.table.column的格式;大小写怎么处理?
			String[] tgtObjs = tgtObj.split("\\.");

			// 先找目标视图所在的位置
			NamedColumnSet simTgtView = new View();
			simTgtView.setName(tgtObjs[1]);
			List<NamedColumnSet> columnSets = cntSchema.getColumnSets();
			int index = Collections.binarySearch(columnSets, simTgtView);
			if (index < 0) {
				continue;
			}
			// index
			// >=0,表示找到视图所在的位置(此处直接认为其来源是表,不是太保险...因其有可能是源自视图!难道...每一个都要去查找一翻....)
			// 这是针对字段级的依赖,而在此处建立的是表一级的依赖,因此会有很多的重复,所幸使用的是Map,其有自动去重功能,故偷懒不考虑去重啦
			View tgtView = (View) columnSets.get(index);
			// 获取该＇源表＇的类型（可以为表或视图）
			NamedColumnSetType srcType = getNamedColumnSetType(srcObj);

			tgtView.addReferenceSchTable(srcObj, srcType);

		}
	}

	private void setDependenciesMacroTable(List<HashMap<String, String>> columnRel) {
		for (HashMap<String, String> cols : columnRel) {
			// 转换成大写形式
			String srcObj = cols.get("Src_Obj").toUpperCase();
			String tgtObj = cols.get("Tgt_Obj").toUpperCase();
			// 分别找到SRC,TGT所在的位置,然后建立它们之间的关系
			// 注意:它们的长度都为3,即为schema.table.column的格式;大小写怎么处理?
			String[] tgtObjs = tgtObj.split("\\.");
			String[] srcObjs = srcObj.split("\\.");

			NamedColumnSetType srcType = getNamedColumnSetType(srcObj);

			// 先找目标宏所在的位置
			List<TdMacro> macros = cntSchema.getMacros();
			for (int i = 0; i < macros.size(); i++) {
				if (macros.get(i).getName().trim().equals(tgtObjs[1])) {
					// 判断宏依赖的表是否存在，如果不存在则记录日志，且不增加到宏所拥有的referenceSchTable中
					List<NamedColumnSet> cntViews = cntSchema.getColumnSets();
					// 排序
					Collections.sort(cntViews);
					NamedColumnSet cView = new View();
					cView.setName(srcObjs[1].toUpperCase());
					TdMacro tgtMacro = macros.get(i);
					tgtMacro.addReferenceSchTable(srcObj, srcType);
					break;
				}
			}
		}
	}

	private NamedColumnSetType getNamedColumnSetType(String srcObj) {

		if (types.containsKey(srcObj)) { return types.get(srcObj); }

		List<Schema> schemas = catalog.getSchemas();
		// 增加非当前schema的视图和表
		List<Schema> schemaLists = new ArrayList<Schema>();
		schemaLists.addAll(schemas);
		schemaLists.addAll(schemaList);
		try {
			String[] srcSchemaTable = srcObj.split("\\.");
			for (Schema schema : schemaLists) {
				if (schema.getName().equals(srcSchemaTable[0])) {
					// 为当前SCHEMA
					List<NamedColumnSet> namedColumnSet = schema.getColumnSets();
					// 1.假定其为表，
					NamedColumnSet srcTable = new Table();
					srcTable.setName(srcSchemaTable[1]);
					int index = Collections.binarySearch(namedColumnSet, srcTable);
					if (index > -1) {
						types.put(srcObj, namedColumnSet.get(index).getType());
						return types.get(srcObj);
					}
					break;
				}
			}
		}
		catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.warn(e);
			}
		}
		// 如果找不到，则默认为表
		types.put(srcObj, NamedColumnSetType.TABLE);
		return NamedColumnSetType.TABLE;
	}

	private void setDependenciesBetweenViewAndTable(List<HashMap<String, String>> columnRel) {

		for (HashMap<String, String> cols : columnRel) {
			// 转换成大写形式
			String srcObj = cols.get("Src_Obj").toUpperCase();
			String tgtObj = cols.get("Tgt_Obj").toUpperCase();
			// 分别找到SRC,TGT所在的位置,然后建立它们之间的关系
			// 注意:它们的长度都为3,即为schema.table.column的格式;大小写怎么处理?
			String[] srcObjs = srcObj.split("\\.");
			String[] tgtObjs = tgtObj.split("\\.");

			// 先找目标视图所在的位置
			NamedColumnSet simTgtView = new View();
			simTgtView.setName(tgtObjs[1]);
			List<NamedColumnSet> columnSets = cntSchema.getColumnSets();
			// 已经排过序，故可以直接查找
			int index = Collections.binarySearch(columnSets, simTgtView);
			if (index < 0) {
				continue;
			}
			// index
			// >=0,表示找到视图所在的位置(此处直接认为其来源是表,不是太保险...因其有可能是源自视图!难道...每一个都要去查找一翻....)
			// 这是针对字段级的依赖,而在此处建立的是表一级的依赖,因此会有很多的重复,所幸使用的是Map,其有自动去重功能,故偷懒不考虑去重啦
			View tgtView = (View) columnSets.get(index);

			// 开始建立视图字段间的关系
			List<Column> tgtViewColumns = tgtView.getColumns();
			for (Column tgtViewColumn : tgtViewColumns) {
				if (tgtViewColumn.getName().equals(tgtObjs[2])) {
					// 获取该＇源表＇的类型（可以为表或视图）
					NamedColumnSetType srcType = getNamedColumnSetType(srcObjs[0].concat(".").concat(srcObjs[1]));
					// 找到视图字段
					tgtViewColumn.addReferenceSchTableColumn(srcObj, srcType);
					break;
				}
			}
		}
	}

	private class TeradataDictionaryDaoImpl implements IDBDictionaryDao {
		public Map<String, MMMetadata> getMdIdCache(){
			return null;
		}
		public Map<String, List<Map<String, String>>> getColInfoFromMM(List<String> views) {
			// 参数表列表为空,则返回空
			if (views == null || views.size() == 0) { return Collections.emptyMap(); }

			Map<String, List<Map<String, String>>> returnColumns = new HashMap<String, List<Map<String, String>>>();
			List<String> tables = new ArrayList<String>();

			for (String unknowName : views) {
				String[] names = unknowName.split("\\.");
				if (names.length == 2) {
					// 带有schema的,它有可能是当前schema的,也有可能不是当前schema的
					String schemaName = names[0];
					String viewName = names[1];

					List<Schema> schemas = catalog.getSchemas();
					boolean findSchema = false;
					for (Schema schema : schemas) {
						if (schema.getName().equals(schemaName.toUpperCase())) {
							// 找到相匹配的
							addColumn(schema, unknowName, viewName, returnColumns);
							findSchema = true;
							break;
						}
					}

					if (!findSchema) {
						// 如果没找到匹配的，就创建Schema和Table
						tables.add(unknowName);
					}
				}
				else {
					String viewName = unknowName;
					addColumn(cntSchema, unknowName, viewName, returnColumns);
				}
			}

			if (tables.size() > 0) {
				try {
					returnColumns.putAll(createNewSchema(tables));
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
			return returnColumns;
		}

		private Map<String, List<Map<String, String>>> createNewSchema(List<String> tables) throws Exception {
			Map<String, List<Map<String, String>>> returnColumns = new HashMap<String, List<Map<String, String>>>();
			Map<String, List<String>> map = new HashMap<String, List<String>>();
			Map<String, String> dupMap = new HashMap<String, String>();
			for (String tableName : tables) {
				if (StringUtils.isEmpty(dupMap.get(tableName.toUpperCase()))) {
					dupMap.put(tableName.toUpperCase(), tableName.toUpperCase());

					String[] tableArr = tableName.split("\\.");
					String scName = tableArr[0].toUpperCase();
					String tabName = tableArr[1].toUpperCase();
					if (map.containsKey(scName)) {
						List<String> tableList = map.get(scName);
						tableList.add(tabName);
					}
					else {
						List<String> list = new ArrayList<String>();
						list.add(tabName);
						map.put(scName, list);
					}
				}
			}

			// 新增schema
			for (Iterator<Entry<String, List<String>>> iter = map.entrySet().iterator(); iter.hasNext();) {
				Entry<String, List<String>> entry = iter.next();
				String schemaName = entry.getKey();
				List<String> tableList = entry.getValue();
				Schema schema = new Schema();
				schema.setName(schemaName.toUpperCase());
				Schema schemaTemp = null;
				boolean find = false;
				// 当前cache中不存在SCHEMA会增加到schemaList中，当schemaList中的schema不包含当前解析的VIEW假如到schema，
				for (Schema s1 : schemaList) {
					if (s1.getName().equals(schemaName.toUpperCase())) {
						List<String> tableList1 = new ArrayList<String>();
						for (String tabN : tableList) {
							NamedColumnSet table = new Table();
							table.setName(tabN.toUpperCase());
							List<NamedColumnSet> ns = s1.getColumnSets();
							Collections.sort(ns);
							int index = Collections.binarySearch(ns, table);
							if (index < 0) {
								tableList1.add(tabN);
							}
						}

						if (tableList1.size() > 0) {
							s1.addColumnSet(newTables(s1.getName(), tableList1));
						}
						schemaTemp = s1;
						find = true;
						break;
					}
					// else {
					// list.add(schema);
					// schema.addColumnSet(newTables(schema.getName(),
					// tableList));
					// }
				}

				// 当schemaList不包含目标schema时增加该schema，再增加该schema的view
				if (schemaList.size() > 0 && !find) {
					schemaList.add(schema);
					schema.addColumnSet(newTables(schema.getName(), tableList));
				}

				// cache中没有schema直接增加schema，再增加该schema的view
				if (schemaList.size() == 0) {
					schemaList.add(schema);
					schema.addColumnSet(newTables(schema.getName(), tableList));
					schemaTemp = schema;
				}

				if (schemaTemp == null) {
					schemaTemp = schema;
				}

				for (String tableName : tableList) {
					addColumn(schemaTemp, schemaTemp.getName() + "." + tableName, tableName, returnColumns);
				}

			}
			return returnColumns;
		}

		private List<NamedColumnSet> newTables(String schName, List<String> tableList) throws SQLException {
			List<NamedColumnSet> tables = getTables(schName, tableList);
			// Map<String, Table> tableCache = new HashMap<String, Table>();
			// Map<String, View> viewCache = new HashMap<String, View>();
			// for (int i = 0; i < tables.size(); i++) {
			// if (tables.get(i) instanceof Table)
			// tableCache.put(tables.get(i).getName(), (Table) tables.get(i));
			// if (tables.get(i) instanceof View)
			// viewCache.put(tables.get(i).getName(), (View) tables.get(i));
			// }
			// if (!tableCache.isEmpty()) {
			// setTableColumns(schName, tableCache);
			// }
			// if (!viewCache.isEmpty()) {
			// setViewColumns(schName, viewCache);
			// }
			return tables;
		}

		private void addColumn(Schema schema, String unknowName, String viewName,
				Map<String, List<Map<String, String>>> returnColumns) {
			// 不带schema的,表示为当前schema
			List<NamedColumnSet> cntViews = schema.getColumnSets();
			// 对views进行排序
			Collections.sort(cntViews);
			NamedColumnSet cView = new View();
			cView.setName(viewName.toUpperCase());
			int index = Collections.binarySearch(cntViews, cView);
			if (index >= 0) {
				// 找到相匹配的
				List<Map<String, String>> columnList = new LinkedList<Map<String, String>>();
				List<Column> columns = cntViews.get(index).getColumns();
				for (Column column : columns) {
					Map<String, String> colMap = new HashMap<String, String>();
					colMap.put("COLUMNNAME", column.getName());
					columnList.add(colMap);
				}
				returnColumns.put(schema.getName().toUpperCase() + "." + viewName.toUpperCase(), columnList);
			}
		}

		public String getTabOriName(String tableName) {
			return tableName;
		}

		public Map<String, String> getTabOriNames(List<String> tableNames) {
			return null;
		}

		public MMMetaModel getTableMetaModel(List<String> tables) {
			return null;
		}

	}

	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		IDBExtractService extractor = new TeradataExtractServiceImpl();
		org.springframework.jdbc.datasource.DriverManagerDataSource dSource = new org.springframework.jdbc.datasource.DriverManagerDataSource();
		// dSource.setDriverClassName("com.ncr.teradata.TeraDriver");
		// dSource.setUrl("jdbc:teradata://172.19.0.158/CLIENT_CHARSET=cp936,TMODE=TERA,CHARSET=ASCII,DATABASE=MM");
		// dSource.setUsername("mmuser");
		// dSource.setPassword("mmuser");
		// extractor.setSchemas("MM");
		// File file = new File("d:\\catalog");
		//
		// ObjectOutput output = new ObjectOutputStream(new
		// FileOutputStream(file));
		//
		// // 保存对象
		//
		// output.writeObject(c);
		//
		// output.close();
		//
		// System.out.println("文件保存在：" + file.getAbsolutePath());
		Log4jInit.init();
		// extractor = new TeradataExtractServiceImpl();
		dSource.setDriverClassName("com.ncr.teradata.TeraDriver");
		dSource.setUrl("jdbc:teradata://172.16.171.24/CLIENT_CHARSET=cp936,TMODE=TERA,CHARSET=ASCII,DATABASE=MM");
		// dSource.setUrl("jdbc:teradata://172.19.166.203/CLIENT_CHARSET=cp936,TMODE=TERA,CHARSET=ASCII,DATABASE=MM");
		dSource.setUsername("mmuser");
		dSource.setPassword("mmuser");
		((DBExtractBaseServiceImpl) extractor).setSchemas("edwbview");
		// ((DBExtractBaseServiceImpl) extractor).setSchemas("mm2");
		// ((DBExtractBaseServiceImpl) extractor).setSchemas("MM");
		JdbcTemplate jdbcTemplate = new JdbcTemplate(dSource);

		((DBExtractBaseServiceImpl) extractor).setJdbcTemplate(jdbcTemplate);
		extractor.getCatalog();

	}
}
