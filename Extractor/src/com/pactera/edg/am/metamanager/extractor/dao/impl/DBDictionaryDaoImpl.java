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

package com.pactera.edg.am.metamanager.extractor.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.IDBDictionaryDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetaModelDao;
import com.pactera.edg.am.metamanager.extractor.ex.MetaModelNotFoundException;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;
import com.pactera.edg.am.metamanager.extractor.util.Utils;

/**
 * MM库表字典操作DAO实现类
 * 
 * @author user
 * @version 1.0 Date: Oct 16, 2009
 */
public class DBDictionaryDaoImpl extends JdbcDaoSupport implements
		IDBDictionaryDao {
	private static Log log = LogFactory.getLog(DBDictionaryDaoImpl.class);

	// 检查元模型中是否有属性columnId的标识
	private static boolean modelHasCheck = false;

	// 元模型中是否有ColumnId
	private static boolean hasColumnId = false;

	// schema,table,column的SQL
	private static String schemaSql, tableSql, columnSql;

	// 字段所处的位置
	private static String columnPosition;

	private IMetaModelDao metaModelDao;

	private Map<String, MMMetadata> mdIdCache = new HashMap<String, MMMetadata>();

	/**
	 * 注入
	 * 
	 * @param metaModelDao
	 */
	public void setMetaModelDao(IMetaModelDao metaModelDao) {
		this.metaModelDao = metaModelDao;
	}

	/**
	 * 缓存，对于一个给定的键，其映射的存在并不阻止垃圾回收器对该键的丢弃
	 */
	private Map<String, List<Map<String, String>>> schemaTablesCache = new WeakHashMap<String, List<Map<String, String>>>(
			0);

	/**
	 * 缓存已经查过的schema
	 */
	private Map<String, String> schemaCache = new WeakHashMap<String, String>(0);

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IDBDictionaryDao#getColInfoFromMM(java.util.List)
	 */
	public Map<String, List<Map<String, String>>> getColInfoFromMM(
			List<String> sqlTables) {
		// 参数表列表为空,则返回空
		if (sqlTables == null || sqlTables.size() == 0) {
			return Collections.emptyMap();
		}

		if (!modelHasCheck) {
			// 只检查模型一次
			hasColumnId = checkModel();
			modelHasCheck = true;
		}
		if (!hasColumnId) {
			// 如没有columnId属性,即表示不能对字段排序,将返回空!!
			return Collections.emptyMap();
		}

		columnSql = GenSqlUtil.getSql("QUERY_DICTIONARY_COLUMN");
		columnSql = columnSql.replaceAll("#COLUMNPOSITION#", columnPosition);
		// 对数据进行排序
		Collections.sort(sqlTables, new Comparator<String>() {

			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});

		// 过滤不符合格式的参数,符合格式的参数:schema.table <schema,Set<table>>
		Set<String> schemaTables = verifyTables(sqlTables);
		if (schemaTables.size() == 0) {
			return Collections.emptyMap();
		}

		// 需要返回的参数
		Map<String, List<Map<String, String>>> returnColumns = new HashMap<String, List<Map<String, String>>>();

		// 根据SCHEMA的CODE获取其命名空间

		for (Iterator<String> schemaTableNames = schemaTables.iterator(); schemaTableNames
				.hasNext();) {
			String schemaTableName = schemaTableNames.next();
			if (schemaTablesCache.containsKey(schemaTableName)) {
				// 在缓存中,表示已经查询过
				if (schemaTablesCache.get(schemaTableName) == null) {
					// 缓存中的结果为null,表示在库表中不存在,此时不放入返回列表中
					continue;
				}
				returnColumns.put(schemaTableName, schemaTablesCache
						.get(schemaTableName));
			} else {
				getTable(schemaTableName, returnColumns);
			}

		}

		if (returnColumns.size() > 0) {
			log.info("成功从元数据库表中获取到字段的库表有:");
		}
		for (Iterator<String> iter = returnColumns.keySet().iterator(); iter
				.hasNext();) {
			log.info(iter.next());
		}
		return returnColumns;
	}

	private void getTable(String schemaTableName,
			Map<String, List<Map<String, String>>> returnColumns) {
		// 检验SCHEMA数据准确性

		String schemaNamespace = genSchemaNamespace(schemaTableName);
		if (schemaNamespace == null) {
			// 没有获取到schema的namespace,或获取到两个以上
			return;
		}

		MMMetadata schemaMetadata = new MMMetadata();
		schemaMetadata.setCode(schemaTableName);
		// SCHEMA数据准确,接着获取表信息
		tableSql = GenSqlUtil.getSql("QUERY_DICTIONARY_TABLE");
		String tmpTableSql = tableSql.replaceAll("#NAMESPACE#", schemaNamespace
				.concat("/%"));
		tmpTableSql = tmpTableSql.replaceAll("#INSTANCE_CODE#", schemaTableName
				.substring(schemaTableName.indexOf(".") + 1).toUpperCase());

		// 获取表的ID
		try {
			List<MMMetadata> tabs = super.getJdbcTemplate().query(tmpTableSql,
					new Object[] {}, new MMMetadataRowMapper());
			
			//2011-8-1修改---yuwei
			if(tabs != null && tabs.size()>0){
				mdIdCache.put(schemaTableName, tabs.get(0));
				List<MMMetadata> columns = super.getJdbcTemplate().query(columnSql,
						new Object[] { tabs.get(0).getId() },
						new MMMetadataRowMapper());
				if (columns == null || columns.size() == 1) {
					return;
				}
				getTableColumns(schemaTableName, columns, returnColumns);
			}
			
		} catch (EmptyResultDataAccessException e) {
			schemaTablesCache.put(schemaTableName, null);
			log.warn(new StringBuilder("元数据库表中不存在表:").append(schemaTableName)
					.append(" 返回结果为空!").toString());
		}

	}

	private class MMMetadataRowMapper implements RowMapper {
		public Object mapRow(ResultSet rs, int arg1) throws SQLException {
			MMMetadata md = new MMMetadata();
			md.setId(rs.getString("INSTANCE_ID"));
			md.setCode(rs.getString("INSTANCE_CODE"));
			md.setName(rs.getString("INSTANCE_NAME"));
			md.setClassifierId(rs.getString("CLASSIFIER_ID"));
			return md;
		}
	}

	private void getTableColumns(String schemaTableName,
			List<MMMetadata> columns,
			Map<String, List<Map<String, String>>> returnColumns) {

		List<Map<String, String>> columnList = new LinkedList<Map<String, String>>();
		for (MMMetadata column : columns) {
			Map<String, String> returnColumn = new HashMap<String, String>();
			String name = column.getCode();
			returnColumn.put("COLUMNNAME", name);
			columnList.add(returnColumn);
			mdIdCache.put(schemaTableName.concat(".") + column.getCode(),
					column);
		}
		// 把结果缓存起来
		schemaTablesCache.put(schemaTableName, columnList);
		returnColumns.put(schemaTableName.toUpperCase(), columnList);

	}

	/**
	 * 获取Schema的命名空间
	 * 
	 * @param schemaTableName
	 * @return
	 */
	private String genSchemaNamespace(String schemaTableName) {
		String schemaName = schemaTableName.substring(0, schemaTableName
				.indexOf("."));

		if (schemaCache.containsKey(schemaName)) {
			// schema已经校验过
			return schemaCache.get(schemaName);
		} else {
			schemaSql = GenSqlUtil.getSql("QUERY_DICTIONARY_SCHEMA");
			List<String> schemasNamespce = (List<String>) super
					.getJdbcTemplate().query(schemaSql,
							new Object[] { schemaName.toUpperCase() },
							new NamespaceRowMapper());
			if (schemasNamespce.size() == 0) {
				// MM库表中没有名为:SCHEMA 的元数据
				log
						.warn("MM库表中没有SCHEMA名为:".concat(schemaName).concat(
								" 的元数据!"));
				schemaCache.put(schemaName, null);
				return null;
			} else if (schemasNamespce.size() > 1) {
				// MM库表中多于一个:SCHEMA 的元数据
				log.warn("MM库表中多于一个SCHEMA名为:".concat(schemaName).concat(
						" 的元数据!取第一个!"));
				// schemaCache.put(schemaName, null);
				// return null;
				schemaCache.put(schemaName, schemasNamespce.get(0));
				return schemasNamespce.get(0);
			}
			log.info(new StringBuilder("Schema:").append(schemaName).append(
					"在元数据库中的命名空间:").append(schemasNamespce.get(0)).toString());
			schemaCache.put(schemaName, schemasNamespce.get(0));
			return schemasNamespce.get(0);
		}

	}

	private boolean checkModel() {
		// ---------metamodel-----------------
		MMMetaModel tableMetaModel = new MMMetaModel();
		tableMetaModel.setCode("Table");
		tableMetaModel.addChildMetaModels("Column");

		MMMetaModel columnMetaModel = tableMetaModel
				.getChildMetaModel("Column");
		try {
			metaModelDao.genStorePositionByApi(columnMetaModel);
			if (!columnMetaModel.getMAttrs().containsKey("columnId")) {
				// 判断属性中是否包含有columnId ??
				log.warn("字段模型中不存在columnId这个属性!!");
				return false;
			}
			columnPosition = columnMetaModel.getMAttrs().get("columnId");

			return true;
		} catch (MetaModelNotFoundException e) {
			e.printStackTrace();
			return false;
		}

		// ----------metamodel end-------------

	}

	private class NamespaceRowMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			return rs.getString("NAMESPACE");
		}

	}

	/**
	 * 获取符合规格的表名:schema.table
	 * 
	 * @param sqlTables
	 * @return
	 */
	private Set<String> verifyTables(List<String> sqlTables) {
		Set<String> schemaTables = new HashSet<String>(sqlTables.size());
		for (String tableName : sqlTables) {
			if (tableName.split("\\.").length == 2) {
				schemaTables.add(tableName);
			} else {
				log.warn("表的格式应为schema.table,不符合格式的表名:" + tableName);
			}
		}
		return schemaTables;
	}

	public String getTabOriName(String tableName) {
		// 经过转换后返回表名
		return Utils.transformName(tableName);
	}

	public Map<String, String> getTabOriNames(List<String> tableNames) {
		// 返回原表列表
		return null;
	}

	public MMMetaModel getTableMetaModel(List<String> tables) {
		return null;
	}

	public Map<String, MMMetadata> getMdIdCache() {
		return mdIdCache;
	}

}
