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

package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.db.IDBExtractService;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Catalog;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ForeignKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Partition;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.PrimaryKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ProcedureColumn;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.TdMacro;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 转换DB模型的数据为公共模型数据的转换实现类
 * 
 * @author user
 * @version 1.0 Date: Jul 23, 2009
 * 
 */
public class DBMappingServiceImpl extends BaseMappingServiceImpl implements
		IMetadataMappingService {

	private Log log = LogFactory.getLog(DBMappingServiceImpl.class);

	/**
	 * DB数据源获取接口
	 */
	private IDBExtractService dbDao;

	/**
	 * 依赖关系
	 */
	private List<MMDDependency> dependencies;

	/**
	 * 缓存与主键作依赖关系的字段元数据
	 */
	private Set<MMMetadata> pkValueMetadataCache;

	/**
	 * 缓存表的字段
	 */
	protected Set<MMMetadata> columnMetadataCache;

	/**
	 * 缓存视图的字段
	 */
	protected Set<MMMetadata> columnMetadataCacheForView;

	/**
	 * 缓存视图字段与其元数据间的相应关系
	 */
	private Map<Column, MMMetadata> viewColumnCache;

	/**
	 * 缓存TD宏与其元数据间的相应关系
	 */
	private Map<TdMacro, MMMetadata> macroCache;

	/**
	 * 缓存视图与其元数据间的相应关系
	 */
	private Map<View, MMMetadata> viewCache;

	/**
	 * 缓存视图的路径Schema.View与相应元数据间的映射
	 */
	private Map<String, MMMetadata> viewViewCache;

	/**
	 * 缓存表:Schema.Table与其元数据间的相应关系
	 */
	private Map<String, MMMetadata> tableCache;

	/**
	 * 缓存与缓存外键与创建的外键元数据间的映射
	 */
	private Map<ForeignKey, MMMetadata> fkMapperMetadataCache;

	/**
	 * 缓存索引与索引元数据间的映射
	 */
	private Map<SQLIndex, MMMetadata> indexMetadataCache;

	/**
	 * 将DB数据源数据对象,转换为公共模型数据
	 */
	public void metadataMapping(AppendMetadata aMetadata) throws Exception {

		try {
			init();
			Catalog catalog = dbDao.getCatalog();
			
			MMMetaModel schemaMetaModel = new MMMetaModel();
			schemaMetaModel.setCode(Schema.class.getSimpleName());
			schemaMetaModel.setParentMetaModel(aMetadata.getMetaModel());

			genSchemaMetaModel(aMetadata.getMetadata(), catalog,
					schemaMetaModel);

			Set<MMMetaModel> metaModels = new HashSet<MMMetaModel>(1);
			metaModels.add(schemaMetaModel);
			aMetadata.setChildMetaModels(metaModels);

			// 建立外键依赖于字段的依赖关系
			addFkDependencies();

			// 建立视图字段间的依赖关系
			addViewDependencies();

			// 建立宏与表的依赖关系
			addMacroDependencies();

			// 建立索引与表,字段间的依赖关系(注意:改为在此建立该依赖关系是因为索引可能作用于其它schema下的表的字段,
			// 故需要先处理了所有的schema的表,字段,然后才开始建立索引与表,字段间的依赖)
			addIndexColumnDependencies();

			aMetadata.setDDependencies(dependencies);
		} finally {
			// 作垃圾清理
			clear();
		}
	}

	private void clear() {
		dependencies = null;
		pkValueMetadataCache = null;
		columnMetadataCache = null;
		columnMetadataCacheForView = null;
		viewColumnCache = null;
		fkMapperMetadataCache = null;
		indexMetadataCache = null;
		viewCache = null;
		macroCache = null;
		tableCache = null;
		viewViewCache = null;
	}

	private void init() {
		dependencies = new ArrayList<MMDDependency>(0);
		pkValueMetadataCache = new HashSet<MMMetadata>(0);
		columnMetadataCache = new HashSet<MMMetadata>();
		columnMetadataCacheForView = new HashSet<MMMetadata>();
		viewColumnCache = new HashMap<Column, MMMetadata>(0);
		fkMapperMetadataCache = new HashMap<ForeignKey, MMMetadata>(0);
		indexMetadataCache = new HashMap<SQLIndex, MMMetadata>(0);
		macroCache = new HashMap<TdMacro, MMMetadata>(0);
		viewCache = new HashMap<View, MMMetadata>(0);
		tableCache = new HashMap<String, MMMetadata>();
		viewViewCache = new HashMap<String, MMMetadata>(0);
	}

	private void addIndexColumnDependencies() {
		try {
			// 缓存索引依赖
			Set<String> duplicate = new HashSet<String>();
			for (Iterator<SQLIndex> indexes = indexMetadataCache.keySet()
					.iterator(); indexes.hasNext();) {
				SQLIndex index = indexes.next();

				String columnName = index.getColumnName();
				String tableName = index.getTableName();
				String schName = index.getSchName();
				// schema1.index_schema2.table,旨在避免重复建立索引与表间的依赖
				String duplcateIndexTable = new StringBuilder(
						indexMetadataCache.get(index).getParentMetadata()
								.getCode()).append(".").append(
						indexMetadataCache.get(index).getCode()).append("_")
						.append(schName).append(".").append(tableName)
						.toString();

				for (MMMetadata columnMetadata : columnMetadataCache) {
					if (columnName.equals(columnMetadata.getCode())
							&& tableName.equals(columnMetadata
									.getParentMetadata().getCode())
							&& schName.equals(columnMetadata
									.getParentMetadata().getParentMetadata()
									.getCode())) {
						// 找到需建立的依赖关系信息
						addDependency(indexMetadataCache.get(index),
								columnMetadata, null, null);
						if (!duplicate.contains(duplcateIndexTable)) {
							addDependency(indexMetadataCache.get(index),
									columnMetadata.getParentMetadata(), null,
									null);
							duplicate.add(duplcateIndexTable);
						}
						break;
					}
				}

			}
		} catch (Exception e) {
			log.warn("在DB转换对象模型部分,建立索引与表/表字段间的依赖时出现异常!", e);
		}
	}

	private void addViewDependencies() {
		addViewDependencies(viewCache, viewViewCache, tableCache);

		addViewColumnDependencies();

	}

	private void addMacroDependencies() {
		addMacroDependencies(macroCache, tableCache, viewViewCache);
	}

	private void addViewColumnDependencies() {

		try {
			for (Iterator<Column> columns = viewColumnCache.keySet().iterator(); columns
					.hasNext();) {
				Column column = columns.next();
				// 获取到它的关联字段
				Map<String, NamedColumnSetType> refTableCols = column
						.getReferenceSchTableColumns();
				for (Iterator<String> tableCols = refTableCols.keySet()
						.iterator(); tableCols.hasNext();) {
					String tableCol = tableCols.next();
					// 视图字段关联的字段可能为视图字段也可能为表的字段
					// if (refTableCols.get(tableCol) ==
					// NamedColumnSetType.TABLE) {
					// 视图字段关联的字段为表的字段
					String[] schTabCol = tableCol.split("\\.");
					if (schTabCol.length != 3) {
						continue;
					}
					Set<MMMetadata> allColumns = new HashSet<MMMetadata>();
					allColumns.addAll(columnMetadataCache);
					allColumns.addAll(columnMetadataCacheForView);
					// 视图字段可能依赖表的字段也可能依赖视图的字段，遍历所有的字段
					for (MMMetadata columnMetadata : allColumns) {
						if (schTabCol[2].equals(columnMetadata.getCode())
								&& schTabCol[1].equals(columnMetadata
										.getParentMetadata().getCode())
								&& schTabCol[0].equals(columnMetadata
										.getParentMetadata()
										.getParentMetadata().getCode())) {
							// 找到需建立的依赖关系信息,建立视图字段与表的字段间的依赖关系
							addDependency(viewColumnCache.get(column),
									columnMetadata, "reportDep", "reportDeped");
							break;
						}
					}
					// }
					// else {
					// 视图字段关联的字段为视图字段 待续....
					// }
				}
			}
		} catch (Exception e) {
			log.warn("在DB转换对象模型部分,建立视图字段与表字段/视图字段间的依赖时出现异常!", e);
		}
	}

	private void addFkDependencies() {
		try {
			for (Iterator<ForeignKey> fkIter = fkMapperMetadataCache.keySet()
					.iterator(); fkIter.hasNext();) {
				ForeignKey fk = fkIter.next();

				// 主键字段名
				String pkColumnName = fk.getPKColumnName();
				// 主键表名
				String pkTableName = fk.getPKTableName();
				// 主键schema名
				String pkSchemaName = fk.getPKSchemaName();
				for (MMMetadata columnMetadata : pkValueMetadataCache) {
					if (pkColumnName.equals(columnMetadata.getCode())
							&& pkTableName.equals(columnMetadata
									.getParentMetadata().getCode())
							&& pkSchemaName.equals(columnMetadata
									.getParentMetadata().getParentMetadata()
									.getCode())) {
						// 找到字段名,表名,schema名都相同的数据,建立依赖关系
						addDependency(fkMapperMetadataCache.get(fk),
								columnMetadata, null, "source");
						// 数据已经建立依赖关系,退出
						break;
					}
				}
			}
		} catch (Exception e) {
			log.warn("在DB转换对象模型部分,建立外键与表字段间的依赖时出现异常!", e);
		}
	}

	private void genSchemaMetaModel(MMMetadata parentMetadata, Catalog catalog,
			MMMetaModel schemaMetaModel) {
		String tableMetaModelName = Table.class.getSimpleName();
		String procedureMetaModelName = Procedure.class.getSimpleName();
		String viewMetaModelName = View.class.getSimpleName();
		String indexMetaModelName = SQLIndex.class.getSimpleName();

		schemaMetaModel.addChildMetaModels(tableMetaModelName,
				procedureMetaModelName, viewMetaModelName, indexMetaModelName);
		List<Schema> schemas = catalog.getSchemas();

		for (Schema schema : schemas) {
			MMMetadata metadata = super.createMetadata(parentMetadata,
					schemaMetaModel, schema.getName(), schema.getName());

			Map<String, MMMetadata> tableMetadatasCache = genTables(metadata,
					schema, schemaMetaModel
							.getChildMetaModel(tableMetaModelName));

			genViews(metadata, schema, schemaMetaModel
					.getChildMetaModel(viewMetaModelName), tableMetadatasCache);

			genProcedures(metadata, schema, schemaMetaModel
					.getChildMetaModel(procedureMetaModelName));

			genSQLIndexs(metadata, schema, schemaMetaModel
					.getChildMetaModel(indexMetaModelName));

			genMacros(metadata, schema, schemaMetaModel);

		}
	}

	private void genMacros(MMMetadata parentMetadata, Schema schema,
			MMMetaModel schemaMetaModel) {
		List<TdMacro> macros = schema.getMacros();
		if (macros.size() == 0) {
			return;
		}

		schemaMetaModel.addChildMetaModel(TdMacro.class.getSimpleName());
		MMMetaModel macroMetaModel = schemaMetaModel
				.getChildMetaModel(TdMacro.class.getSimpleName());

		for (TdMacro macro : macros) {
			MMMetadata metadata = super.createMetadata(parentMetadata,
					macroMetaModel, macro.getName(), null);

			Map<String, String> attrs = macro.getAttrs();
			metadata.setAttrs(attrs);
			if (attrs.containsKey(ModelElement.REMARKS)) {
				String remarks = attrs.get(ModelElement.REMARKS);
				// 如果属性包含注释，但注释为空则设置元数据的名称与代码相同
				if (remarks != null && remarks.trim().length() > 0) {
					metadata.setName(remarks);
				} else {
					metadata.setName(macro.getName());
				}
				setAttr(metadata, "comment", attrs.get(ModelElement.REMARKS));
			}

			// 缓存宏
			macroCache.put(macro, metadata);
		}
	}

	private void genSQLIndexs(MMMetadata parentMetadata, Schema schema,
			MMMetaModel indexMetaModel) {
		List<SQLIndex> indexs = schema.getSQLIndexs();
		if (indexs.size() == 0) {
			return;
		}

		// 缓存索引的名称与其元数据
		Map<String, MMMetadata> map = new HashMap<String, MMMetadata>(indexs
				.size());
		for (SQLIndex index : indexs) {
			String indexName = index.getName();
			if (map.containsKey(indexName)) {
				// 缓存索引与索引元数据间的映射
				indexMetadataCache.put(index, map.get(indexName));
				// log.warn("索引重复:" + indexName);
				// addIndexDependency(map.get(indexName), index, false);
				continue;
			}

			MMMetadata metadata = super.createMetadata(parentMetadata,
					indexMetaModel, index.getName(), null);

			metadata.setAttrs(index.getAttrs());

			// 缓存索引与索引元数据间的映射
			indexMetadataCache.put(index, metadata);
			map.put(indexName, metadata);

			// addIndexDependency(metadata, index, true);
		}
	}

	// private void addIndexDependency(MMMetadata indexMetadata, SQLIndex index,
	// boolean parentNeedDep) {
	// String columnName = index.getColumnName();
	// String tableName = index.getTableName();
	// for (MMMetadata columnMetadata : columnMetadataCache) {
	// if (columnName.equals(columnMetadata.getCode())
	// && tableName.equals(columnMetadata.getParentMetadata().getCode())) {
	// // 找到需建立的依赖关系信息
	// addDependency(indexMetadata, columnMetadata, null, null);
	// if (parentNeedDep) {
	// addDependency(indexMetadata, columnMetadata.getParentMetadata(), null,
	// null);
	// }
	//
	// break;
	// }
	// }
	//
	// }

	private void genViews(MMMetadata parentMetadata, Schema schema,
			MMMetaModel viewMetaModel,
			Map<String, MMMetadata> tableMetadatasCache) {
		String columnMetaModelName = Column.class.getSimpleName();

		viewMetaModel.addChildMetaModels(columnMetaModelName);

		// 缓存视图的元数据信息
		// Map<View, MMMetadata> viewMetadatasCache1 = new HashMap<View,
		// MMMetadata>();
		// Map<String, MMMetadata> viewMetadatasCache2 = new HashMap<String,
		// MMMetadata>();

		List<NamedColumnSet> namedColumnSet = schema.getColumnSets();
		boolean hasView = false;

		for (NamedColumnSet namedColumn : namedColumnSet) {

			if (namedColumn.getType() == NamedColumnSetType.VIEW) {
				hasView = true;
				MMMetadata metadata = super.createMetadata(parentMetadata,
						viewMetaModel, namedColumn.getName(), null);

				Map<String, String> attrs = namedColumn.getAttrs();
				metadata.setAttrs(attrs);
				if (attrs.containsKey(ModelElement.REMARKS)) {
					String remarks = attrs.get(ModelElement.REMARKS);
					// 如果属性包含注释，但注释为空则设置元数据的名称与代码相同
					if (remarks != null && remarks.trim().length() > 0) {
						metadata.setName(remarks);
					} else {
						metadata.setName(namedColumn.getName());
					}
					setAttr(metadata, "comment", attrs
							.get(ModelElement.REMARKS));
				}
				String text = namedColumn.getAttrs().get(View.SQL);
				metadata.addAttr(View.SQL.toLowerCase(), text);

				genColumns(metadata, namedColumn, viewMetaModel
						.getChildMetaModel(columnMetaModelName));

				// 缓存视图
				viewCache.put((View) namedColumn, metadata);
				viewViewCache.put(metadata.getParentMetadata().getCode()
						.concat(".").concat(metadata.getCode()), metadata);
				// viewMetadatasCache1.put((View) namedColumn, metadata);
				// viewMetadatasCache2.put(metadata.getParentMetadata().getCode().concat(".").concat(metadata.getCode()),
				// metadata);
			}

		}
		if (hasView) {
			viewMetaModel.setHasMetadata(true);

			// 添加视图与视图之间的依赖关系,视图与表之间的依赖关系
			// addViewDependencies(viewMetadatasCache1, viewMetadatasCache2,
			// tableMetadatasCache);
		}

	}

	/**
	 * 1.建立宏与表的依赖关系;
	 * 
	 * @param macroMetadatasCache
	 * @param tableMetadatasCache
	 */
	private void addMacroDependencies(
			Map<TdMacro, MMMetadata> macroMetadatasCache,
			Map<String, MMMetadata> tableMetadatasCache,
			Map<String, MMMetadata> viewMetadatasCache) {
		try {
			for (Iterator<TdMacro> macroIter = macroMetadatasCache.keySet()
					.iterator(); macroIter.hasNext();) {
				// 宏,它的VALUE为依赖端
				TdMacro macro = macroIter.next();
				Map<String, NamedColumnSetType> references = macro
						.getReferenceSchTables();
				for (Iterator<String> reference = references.keySet()
						.iterator(); reference.hasNext();) {
					// 依赖的表或视图的schema.table
					String schTableName = reference.next();

					// log.warn("源对象：" + schTableName + ",类型:"
					// + references.get(schTableName));

					if (references.get(schTableName) == NamedColumnSetType.VIEW) {
						if (viewMetadatasCache.containsKey(schTableName)) {

							// 找到视图依赖的视图,建立它们间的依赖关系
							addDependency(macroMetadatasCache.get(macro),
									viewMetadatasCache.get(schTableName),
									"reportDep", "reportDeped");

							// 视图与视图间建立了依赖,才谈得上它们之间的字段有依赖

						}
					} else {
						if (tableMetadatasCache.containsKey(schTableName)) {

							// 找到宏依赖的表,建立它们间的依赖关系
							addDependency(macroMetadatasCache.get(macro),
									tableMetadatasCache.get(schTableName),
									"reportDep", "reportDeped");

							// 视图与表之间建立了依赖,才谈得上它们之间的字段有依赖

						}
					}

				}
			}
		} catch (Exception e) {
			log.warn("在DB转换对象模型部分,建立宏与表的依赖时出现异常!", e);

		}

	}

	/**
	 * 1.建立视图与视图间的依赖关系;包括字段; 2.建立视图与表之间的依赖关系;包括字段.
	 * 
	 * @param viewMetadatasCache1
	 * @param viewMetadatasCache2
	 * @param tableMetadatasCache
	 */
	private void addViewDependencies(Map<View, MMMetadata> viewMetadatasCache1,
			Map<String, MMMetadata> viewMetadatasCache2,
			Map<String, MMMetadata> tableMetadatasCache) {
		try {
			for (Iterator<View> viewIter = viewMetadatasCache1.keySet()
					.iterator(); viewIter.hasNext();) {
				// 视图,它的VALUE为依赖端
				View view = viewIter.next();
				Map<String, NamedColumnSetType> references = view
						.getReferenceSchTables();
				for (Iterator<String> reference = references.keySet()
						.iterator(); reference.hasNext();) {
					// 依赖的表或视图的schema.table
					String schTableName = reference.next();
					if (references.get(schTableName) == NamedColumnSetType.VIEW) {
						if (viewMetadatasCache2.containsKey(schTableName)) {
							// 找到视图依赖的视图,建立它们间的依赖关系
							addDependency(viewMetadatasCache1.get(view),
									viewMetadatasCache2.get(schTableName),
									"reportDep", "reportDeped");

							// 视图与视图间建立了依赖,才谈得上它们之间的字段有依赖

						}
					} else {
						if (tableMetadatasCache.containsKey(schTableName)) {
							// 找到视图依赖的表,建立它们间的依赖关系
							addDependency(viewMetadatasCache1.get(view),
									tableMetadatasCache.get(schTableName),
									"reportDep", "reportDeped");

							// 视图与表之间建立了依赖,才谈得上它们之间的字段有依赖

						}
					}
				}
			}
		} catch (Exception e) {
			log.warn("在DB转换对象模型部分,建立视图与表/视图间的依赖时出现异常!", e);

		}

	}

	private void genProcedures(MMMetadata parentMetadata, Schema schema,
			MMMetaModel procedureMetaModel) {
		String pColumnName = ProcedureColumn.class.getSimpleName();
		procedureMetaModel.addChildMetaModels(pColumnName);

		List<Procedure> procedures = schema.getProcedures();
		int size = procedures.size();
		// String metaModelCode = procedureMetaModel.getCode();
		Set<String> procedureCache = new HashSet<String>(size);
		for (int i = 0; i < size; i++) {
			Procedure procedure = procedures.get(i);
			String procedureName = procedure.getName();
			if (procedureCache.contains(procedureName)) {
				log.warn("已经存在存储过程名:" + procedureName);
				continue;
			}
			procedureCache.add(procedureName);

			MMMetadata metadata = super.createMetadata(parentMetadata,
					procedureMetaModel, procedureName, null);
			metadata.setAttrs(procedure.getAttrs());
			metadata.addAttr(Procedure.TEXT, procedure.getText());

			genProcedureColumns(metadata, procedure, procedureMetaModel
					.getChildMetaModel(pColumnName));

		}
	}

	private void genProcedureColumns(MMMetadata parentMetadata,
			Procedure procedure, MMMetaModel procedureColumnMetaModel) {
		List<ProcedureColumn> pColumns = procedure.getPColumns();
		if (pColumns == null || pColumns.size() == 0) {
			return;
		}

		Set<String> set = new HashSet<String>();
		for (ProcedureColumn pColumn : pColumns) {

			if (set.contains(pColumn.getName())) {
				log.warn("存储过程:" + parentMetadata.getCode() + ",参数重复:"
						+ pColumn.getName());
				continue;
			}
			set.add(pColumn.getName());

			MMMetadata metadata = super.createMetadata(parentMetadata,
					procedureColumnMetaModel, pColumn.getName(), null);
			metadata.setAttrs(pColumn.getAttrs());
		}
	}

	protected Map<String, MMMetadata> genTables(MMMetadata parentMetadata,
			Schema schema, MMMetaModel tableMetaModel) {
		String columnMetaModelName = Column.class.getSimpleName();
		String primaryKeyMetaModelName = PrimaryKey.class.getSimpleName();
		String fkMetaModelName = ForeignKey.class.getSimpleName();
		String pMetaModelName = Partition.class.getSimpleName();

		tableMetaModel.addChildMetaModels(columnMetaModelName,
				primaryKeyMetaModelName, fkMetaModelName, pMetaModelName);

		List<NamedColumnSet> namedColumnSet = schema.getColumnSets();

		/**
		 * 缓存<schname.tabname,tableMetadata>,为后续视图与表间建关系作准备
		 */
		Map<String, MMMetadata> tableMetadatasCache = new HashMap<String, MMMetadata>();

		for (NamedColumnSet namedColumn : namedColumnSet) {

			if (namedColumn.getType() == NamedColumnSetType.TABLE) {
				// TABLE
				Table table = (Table) namedColumn;
				MMMetadata metadata = super.createMetadata(parentMetadata,
						tableMetaModel, namedColumn.getName(), null);

				Map<String, String> attrs = table.getAttrs();
				metadata.setAttrs(attrs);
				if (attrs.containsKey(ModelElement.REMARKS)) {
					String remarks = attrs.get(ModelElement.REMARKS);
					// 如果属性包含注释，但注释为空则设置元数据的名称与代码相同
					if (remarks != null && remarks.trim().length() > 0) {
						metadata.setName(remarks);
					} else {
						metadata.setName(namedColumn.getName());
					}
					setAttr(metadata, "comment", attrs
							.get(ModelElement.REMARKS));
				}
				setAttr(metadata, "nextExtent","重要");
				
				List<MMMetadata> columnMetadatas = genColumns(metadata, table,
						tableMetaModel.getChildMetaModel(columnMetaModelName));

				// 将字段缓存起来
				columnMetadataCache.addAll(columnMetadatas);
				// 将表缓存起来
				tableCache.put(metadata.getParentMetadata().getCode().concat(
						".").concat(metadata.getCode()), metadata);
				tableMetadatasCache.put(metadata.getParentMetadata().getCode()
						.concat(".").concat(metadata.getCode()), metadata);
				// 主键
				genPrimaryKeys(metadata, table, tableMetaModel
						.getChildMetaModel(primaryKeyMetaModelName),
						columnMetadatas);

				// 外键
				genForeignKeys(metadata, table, tableMetaModel
						.getChildMetaModel(fkMetaModelName), columnMetadatas);

				// 表分区
				genPartitions(metadata, table, tableMetaModel
						.getChildMetaModel(pMetaModelName));

			}

		}

		return tableMetadatasCache;

	}

	protected void genPartitions(MMMetadata metadata, Table table,
			MMMetaModel childMetaModel) {

	}

	protected void genForeignKeys(MMMetadata parentMetadata, Table table,
			MMMetaModel fkMetaModel, List<MMMetadata> columnMetadatas) {
		List<ForeignKey> fks = table.getForeignKeies();
		int size = fks.size();

		Map<String, MMMetadata> map = new HashMap<String, MMMetadata>();
		for (int i = 0; i < size; i++) {
			ForeignKey fk = fks.get(i);
			String fkName = fk.getName();
			if (map.containsKey(fkName)) {
				// 添加字段依赖于外键
				String columnName = fk.getFKColumnName();
				addColumnDependencyFK(map.get(fkName), columnName,
						columnMetadatas);
				// log.warn("重复的外键,表名:" + table.getName() + ",外键:" + fkName);

				// 缓存与缓存外键与创建的外键元数据间的映射
				fkMapperMetadataCache.put(fk, map.get(fkName));
				continue;
			}

			MMMetadata metadata = super.createMetadata(parentMetadata,
					fkMetaModel, fkName, null);

			map.put(fkName, metadata);
			// 缓存与缓存外键与创建的外键元数据间的映射
			fkMapperMetadataCache.put(fk, metadata);

			// 添加字段依赖于外键
			String columnName = fk.getFKColumnName();
			addColumnDependencyFK(metadata, columnName, columnMetadatas);

		}
	}

	private void addColumnDependencyFK(MMMetadata fkMetadata,
			String fkColumnName, List<MMMetadata> columnMetadatas) {

		for (MMMetadata columnMetadata : columnMetadatas) {
			if (fkColumnName.equals(columnMetadata.getCode())) {
				addDependency(columnMetadata, fkMetadata, null, "target");
				break;
			}
		}
	}

	protected void genPrimaryKeys(MMMetadata parentMetadata, Table table,
			MMMetaModel primaryKeyMetaModel, List<MMMetadata> columnMetadatas) {
		List<PrimaryKey> primaryKeys = table.getPrimaryKeies();
		int size = primaryKeys.size();
		Map<String, MMMetadata> map = new HashMap<String, MMMetadata>();

		for (int i = 0; i < size; i++) {
			PrimaryKey primaryKey = primaryKeys.get(i);
			String pkName = primaryKey.getName();
			if (map.containsKey(pkName)) {
				// log.warn("重复的主键名:" + pkName + ",表名:" +
				// parentMetadata.getCode());
				// 添加依赖关系
				String columnName = primaryKey.getColumnName();
				addPKDependency(map.get(pkName), columnName, columnMetadatas);
				continue;
			}

			MMMetadata metadata = super.createMetadata(parentMetadata,
					primaryKeyMetaModel, pkName, null);
			metadata.setAttrs(primaryKey.getAttrs());

			// 缓存主键
			map.put(pkName, metadata);

			// 添加依赖关系
			String columnName = primaryKey.getColumnName();
			addPKDependency(metadata, columnName, columnMetadatas);

		}
	}

	private void addPKDependency(MMMetadata pkMetadata, String pkColumnName,
			List<MMMetadata> columnMetadatas) {
		for (MMMetadata columnMetadata : columnMetadatas) {
			if (pkColumnName.equals(columnMetadata.getCode())) {

				addDependency(pkMetadata, columnMetadata, null, null);
				// 缓存与主键作依赖关系的字段,为后续与外键建立关系作准备
				pkValueMetadataCache.add(columnMetadata);
				break;
			}
		}
	}

	private void addDependency(MMMetadata ownerMetadata,
			MMMetadata valueMetadata, String oRole, String vRole) {
		MMDDependency dependency = new MMDDependency();
		dependency.setOwnerMetadata(ownerMetadata);
		dependency.setValueMetadata(valueMetadata);
		if (oRole != null) {
			dependency.setOwnerRole(oRole);
		}
		if (vRole != null) {
			dependency.setValueRole(vRole);
		}
		dependencies.add(dependency);
	}

	/**
	 * @param parentMetadata
	 * @param table
	 * @param columnMetaModel
	 */
	protected List<MMMetadata> genColumns(MMMetadata parentMetadata,
				NamedColumnSet namedColumn, MMMetaModel columnMetaModel) {
		List<Column> columns = namedColumn.getColumns();
		if (columns == null || columns.size() == 0) {
			log.warn("视图或表的字段为空:" + parentMetadata.getCode());
			return new ArrayList<MMMetadata>(0);
		}

		NamedColumnSetType type = namedColumn.getType();
		// 缓存该表所有字段,为后续创建与主键,外键间的依赖关系作铺垫
		List<MMMetadata> columnMetadatas = new ArrayList<MMMetadata>(columns
				.size());
		for (Column column : columns) {
			MMMetadata metadata = mappingColumn(parentMetadata, columnMetaModel, column);
			columnMetadatas.add(metadata);

			if (type == NamedColumnSetType.VIEW) {
				// 是视图,则缓存视图字段
				viewColumnCache.put(column, metadata);
				columnMetadataCacheForView.add(metadata);
			}
		}
		return columnMetadatas;
	}
	
	protected MMMetadata mappingColumn(MMMetadata parentMetadata, MMMetaModel columnMetaModel, Column column) {
		MMMetadata metadata = super.createMetadata(parentMetadata,
				columnMetaModel, column.getName(), null);

		Map<String, String> attrs = column.getAttrs();
		if (attrs.containsKey(ModelElement.REMARKS)) {
			metadata.setName(attrs.get(ModelElement.REMARKS));
		}
		setAttr(metadata, "comment", attrs.get(ModelElement.REMARKS));
		setAttr(metadata, "definition", column.getColumnDef());
		setAttr(metadata, "sqlSimpleType", column.getTypeName());
		setAttr(metadata, "collengths", String.valueOf(column.getColumnSize()));
		setAttr(metadata, "scale", column.getDecimalDigits() != null ? 
				String.valueOf(column.getDecimalDigits()) : null);
		setAttr(metadata, "precision", column.getCharOctetLength() != null ?
				String.valueOf(column.getCharOctetLength()) : null);
		setAttr(metadata, "columnId", String.valueOf(column.getOrdinalPosition()));
		setAttr(metadata, "isNullable", column.isNullable() != null &&
				column.isNullable().booleanValue() ? "YES" : "NO");
		setAttr(metadata, "initialValue", column.getColumnDef());
		setAttr(metadata, "format", column.getFormat());
		setAttr(metadata, "characterSetName", column.getCharacterSetName());
		Boolean compressible = column.getCompressible();
		setAttr(metadata, "compressible", compressible != null &&
				compressible.booleanValue() ? "YES" : "");
		return metadata;
	}
	

	protected void setAttr(MMMetadata metadata, String key, String value) {
		if (value != null) {
			metadata.addAttr(key, value);
		}
	}

	public void setDbDao(IDBExtractService dbDao) {
		this.dbDao = dbDao;
	}

}
