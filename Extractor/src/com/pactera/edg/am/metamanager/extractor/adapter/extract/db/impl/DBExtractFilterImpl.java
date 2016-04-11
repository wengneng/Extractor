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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.IExtractFilter;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Catalog;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ForeignKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.SQLIndex;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;
import com.pactera.edg.am.metamanager.extractor.util.TransformConfLoader;
import com.pactera.edg.am.metamanager.extractor.util.Utils;

public class DBExtractFilterImpl implements IExtractFilter {

	private Log log = LogFactory.getLog(DBExtractFilterImpl.class);

	/**
	 * 需过滤转换的表名映射：＜源SCHEMA.源表名，转换后的表名＞
	 */
	private Map<String, String> filtrateTables;

	/**
	 * 对表进行转换过滤
	 * 
	 * @param catalog
	 */
	public void filtrate(Catalog catalog) {
		// 1.先判断一下配置文件是否存在
		try {
			TransformConfLoader.getProperties();
			// 同时应判断下是否有条件规则，如一个也没有，应即刻返回，待续．．．
		}
		catch (IOException e) {
			// 配置文件不存在,返回不处理
			log.warn(e);
			return;
		}
		log.info("开始对表名作转换...");

		filtrateTables = new HashMap<String, String>();
		List<Schema> schemas = catalog.getSchemas();
		try {
			for (Schema schema : schemas) {
				filtrateTable(schema);

			}

			// TODO 为了避免造成集很多索引于表一身的效果，此处先单独转换表名，同时缓存所有表名与转换后的表名
			// 待完成后，再开始处理索引，外键，视图等．处理这些元素时，只对这些元素所关联的表存在于缓存中的元素，才作处理，否则应清除这些元素
			// 是否会有问题？对于一个表也没有关联的情况．．．不清除之！
			for (Schema schema : schemas) {
				filtrateNamedColumnSet(schema);
				filtrateIndex(schema);
			}
			log.info("对表名作转换结束！");
		}
		catch (Exception e) {
			log.warn("对表名作转换时发生异常！", e);
		}
		finally {
			filtrateTables = null;
		}
	}

	private void filtrateNamedColumnSet(Schema schema) {
		List<NamedColumnSet> namedColumnSets = schema.getColumnSets();

		for (NamedColumnSet namedColumnSet : namedColumnSets) {
			if (namedColumnSet.getType() == NamedColumnSetType.TABLE) {
				filtrateFK((Table) namedColumnSet);
			}
			else {
				// 视图
				filtrateView((View) namedColumnSet);
				filtrateViewColumn((View) namedColumnSet);

			}
		}
	}

	private void filtrateViewColumn(View view) {

		List<Column> vColumns = view.getColumns();

		for (Column vColumn : vColumns) {
			Map<String, NamedColumnSetType> refSchTableColumns = vColumn.getReferenceSchTableColumns();
			if (refSchTableColumns.size() == 0)
				continue;

			Map<String, String> filtrateColumns = new HashMap<String, String>();
			for (Iterator<Entry<String, NamedColumnSetType>> schTableColumns = refSchTableColumns.entrySet().iterator(); schTableColumns
					.hasNext();) {
				Entry<String, NamedColumnSetType> schTableColumn = schTableColumns.next();
				if (schTableColumn.getValue() == NamedColumnSetType.TABLE) {
					int index = schTableColumn.getKey().lastIndexOf(".");
					String schTable = schTableColumn.getKey().substring(0, index);
					if (filtrateTables.containsKey(schTable)) {
						String filtrateTableName = filtrateTables.get(schTable);
						filtrateTableName = schTable.substring(0, schTable.indexOf(".") + 1)
								.concat(filtrateTableName);
						if (!schTable.equals(filtrateTableName)) {
							filtrateColumns.put(schTableColumn.getKey(), filtrateTableName.concat(schTableColumn
									.getKey().substring(index)));
						}
					}
					else {
						filtrateColumns.put(schTableColumn.getKey(), null);
					}
				}
			}

			for (Iterator<String> filtrateColumnIter = filtrateColumns.keySet().iterator(); filtrateColumnIter
					.hasNext();) {
				String filtrateColumn = filtrateColumnIter.next();
				refSchTableColumns.remove(filtrateColumn);
				if (filtrateColumns.get(filtrateColumn) != null) {
					refSchTableColumns.put(filtrateColumns.get(filtrateColumn), NamedColumnSetType.TABLE);
				}
			}
		}
	}

	private void filtrateView(View view) {
		Map<String, NamedColumnSetType> rSchTables = view.getReferenceSchTables();
		if (rSchTables.size() == 0)
			return;

		Set<String> filtrateRTable = new HashSet<String>();

		for (Iterator<Entry<String, NamedColumnSetType>> schTables = rSchTables.entrySet().iterator(); schTables
				.hasNext();) {
			Entry<String, NamedColumnSetType> schTable = schTables.next();
			if (schTable.getValue() == NamedColumnSetType.TABLE) {
				// 视图关联的是表,则先将其缓存起来
				filtrateRTable.add(schTable.getKey());

			}

		}

		for (String srcTableName : filtrateRTable) {
			rSchTables.remove(srcTableName);
			String srcSchemaName = srcTableName.substring(0, srcTableName.indexOf(".") + 1);
			if (filtrateTables.containsKey(srcTableName)) {
				rSchTables.put(srcSchemaName.concat(filtrateTables.get(srcTableName)), NamedColumnSetType.TABLE);
			}
		}

	}

	private void filtrateIndex(Schema schema) {
		List<SQLIndex> indexes = schema.getSQLIndexs();
		List<SQLIndex> faltrateIndexes = new ArrayList<SQLIndex>();

		for (SQLIndex index : indexes) {

			String depSchemaName = index.getSchName();
			String depTableName = index.getTableName();
			if (depSchemaName == null || depSchemaName.equals("") || depTableName == null || depTableName.equals("")) {
				// 所关联的SCHEMA.TABLE为空，不能确定该索引与哪个表关联，此时应将该索引纳入进来（不能错杀）
				faltrateIndexes.add(index);
			}
			else {
				String depSchTableName = depSchemaName.concat(".").concat(depTableName);
				if (filtrateTables.containsKey(depSchTableName)) {
					// 转换索引所关联的表名
					index.setTableName(filtrateTables.get(depSchTableName));
					faltrateIndexes.add(index);
				}
				else {
					// 所关联的表不在转换列表中，即表示该索引所关联的表，不在需要入元数据库的表的列表中，那么该索引应该去除，即不加到faltrateIndexes列表中
				}
			}
		}

		schema.setSQLIndexs(faltrateIndexes);
	}

	private void filtrateTable(Schema schema) {
		List<NamedColumnSet> namedColumnSets = schema.getColumnSets();
		// 缓存转换后的表名
		Set<String> duplicate = new HashSet<String>();
		List<NamedColumnSet> faltrateNamedColumnSets = new ArrayList<NamedColumnSet>();
		String schemaName = schema.getName();

		for (NamedColumnSet namedColumnSet : namedColumnSets) {
			if (namedColumnSet.getType() == NamedColumnSetType.TABLE) {
				// 是个表
				String tableName = namedColumnSet.getName();
				// 转换后的表名
				String falterTableName = Utils.transformName(tableName);
				if (!duplicate.contains(tableName)) {
					// 转换后的表名还不存在于缓存中
					// 表名采用转换后的表名
					namedColumnSet.setName(falterTableName);

					faltrateNamedColumnSets.add(namedColumnSet);
					duplicate.add(tableName);

					// 缓存源表名与转换后的表名的映射关系
					filtrateTables.put(schemaName.concat(".").concat(tableName), falterTableName);
				}
				else {
					// 如果转换后的表名已经在缓存列表中，则该表丢弃
				}

			}
			else {
				// 视图
				faltrateNamedColumnSets.add(namedColumnSet);
			}
		}

		// 去除旧的表,把经转换表名后的表列表放进来
		schema.setColumnSets(faltrateNamedColumnSets);

	}

	private void filtrateFK(Table table) {
		List<ForeignKey> fks = table.getForeignKeies();
		List<ForeignKey> filtrateFKs = new ArrayList<ForeignKey>();

		for (ForeignKey fk : fks) {
			String pkSchName = fk.getPKSchemaName();
			String pkTableName = fk.getPKTableName();
			if (pkSchName == null || pkSchName.equals("") || pkTableName == null || pkTableName.equals("")) {
				// 该外键所关联的SCHEMA.TABLE为空，不能确定该外键与哪个表关联，此时应将该外键纳入进来（不能错杀）
				filtrateFKs.add(fk);
			}
			else {
				String pkSchTableName = pkSchName.concat(".").concat(pkTableName);
				if (filtrateTables.containsKey(pkSchTableName)) {
					// 转换外键关联的PK表名
					fk.setPKTableName(filtrateTables.get(pkSchTableName));
					filtrateFKs.add(fk);
				}
				else {
					// 所关联的表不在转换列表中，即表示该外键所关联的表，不在需要入元数据库的表的列表中，那么该外键应该去除，即不加到filtrateFKs列表中
				}
			}
		}
		table.setForeignKeies(filtrateFKs);
	}

}
