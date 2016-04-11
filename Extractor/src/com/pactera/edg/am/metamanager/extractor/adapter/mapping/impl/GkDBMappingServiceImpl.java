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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.ForeignKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Partition;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.PrimaryKey;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 国开行要求comment放置于属性中,但不放置于元数据name中
 * 
 * @author 汉清陈
 * @version 1.0 Date: Mar 24, 2010
 * 
 */
public class GkDBMappingServiceImpl extends DBMappingServiceImpl {

	private Log log = LogFactory.getLog(GkDBMappingServiceImpl.class);
	
	
	/**
	 * 默认情况下name放的是comment（中文名），但在国开项目中name直接放字段的代码
	 */
	protected MMMetadata mappingColumn(MMMetadata parentMetadata,
			MMMetaModel columnMetaModel, Column column) {
		MMMetadata metadata = super.mappingColumn(parentMetadata, columnMetaModel, column);
		metadata.setName(column.getName());
		return metadata;
	}

	/**
	 * 
	 * 
	 * @param parentMetadata
	 * @param table
	 * @param columnMetaModel
	 */
	protected List<MMMetadata> genColumns(MMMetadata parentMetadata, NamedColumnSet namedColumn,
			MMMetaModel columnMetaModel) {

		
		
		List<Column> columns = namedColumn.getColumns();
		if (columns == null || columns.size() == 0) {
			log.warn("视图或表的字段为空:" + parentMetadata.getCode());
			return new ArrayList<MMMetadata>(0);
		}

		// 缓存该表所有字段,为后续创建与主键,外键间的依赖关系作铺垫
		List<MMMetadata> columnMetadatas = new ArrayList<MMMetadata>(columns.size());
		for (Column column : columns) {
			MMMetadata metadata = new MMMetadata();
			metadata.setCode(column.getName());

			Map<String, String> attrs = column.getAttrs();
			metadata.setParentMetadata(parentMetadata);
			metadata.setClassifierId(columnMetaModel.getCode());
			setAttr(metadata, "comment", attrs.get(ModelElement.REMARKS));
			setAttr(metadata, "definition", column.getColumnDef());
			setAttr(metadata, "sqlSimpleType", column.getTypeName());
			setAttr(metadata, "collengths", String.valueOf(column.getColumnSize()));
			setAttr(metadata, "scale", String.valueOf(column.getDecimalDigits()));
			setAttr(metadata, "precision", String.valueOf(column.getCharOctetLength()));
			setAttr(metadata, "columnId", String.valueOf(column.getOrdinalPosition()));
			setAttr(metadata, "isNullable", column.isNullable() ? "YES" : "NO");
			setAttr(metadata, "initialValue", column.getColumnDef());
			setAttr(metadata, "format", column.getFormat());
			setAttr(metadata, "characterSetName", column.getCharacterSetName());
			Boolean compressible = column.getCompressible();
			setAttr(metadata, "compressible", compressible != null && compressible.booleanValue() ? "YES" : "");
			
			columnMetaModel.addMetadata(metadata);
			columnMetadatas.add(metadata);
		}
		if (columns.size() > 0) {
			columnMetaModel.setHasMetadata(true);
		}
		return columnMetadatas;

	}

	protected Map<String, MMMetadata> genTables(MMMetadata parentMetadata, Schema schema, MMMetaModel tableMetaModel) {
		String columnMetaModelName = Column.class.getSimpleName();
		String primaryKeyMetaModelName = PrimaryKey.class.getSimpleName();
		String fkMetaModelName = ForeignKey.class.getSimpleName();
		String pMetaModelName = Partition.class.getSimpleName();

		tableMetaModel
				.addChildMetaModels(columnMetaModelName, primaryKeyMetaModelName, fkMetaModelName, pMetaModelName);

		List<NamedColumnSet> namedColumnSet = schema.getColumnSets();

		/**
		 * 缓存<schname.tabname,tableMetadata>,为后续视图与表间建关系作准备
		 */
		Map<String, MMMetadata> tableMetadatasCache = new HashMap<String, MMMetadata>();

		for (NamedColumnSet namedColumn : namedColumnSet) {

			if (namedColumn.getType() == NamedColumnSetType.TABLE) {
				// TABLE
				Table table = (Table) namedColumn;
				MMMetadata metadata = new MMMetadata();
				metadata.setCode(namedColumn.getName());
				metadata.setParentMetadata(parentMetadata);

				Map<String, String> attrs = table.getAttrs();
				metadata.setAttrs(attrs);
				if (attrs.containsKey(ModelElement.REMARKS)) {
					setAttr(metadata, "comment", attrs.get(ModelElement.REMARKS));
				}
				// if (attrs.containsKey(ModelElement.REMARKS)) {
				// metadata.setName(attrs.get(ModelElement.REMARKS));
				// }
				metadata.setClassifierId(tableMetaModel.getCode());
				List<MMMetadata> columnMetadatas = genColumns(metadata, table, tableMetaModel
						.getChildMetaModel(columnMetaModelName));

				// 将字段缓存起来
				columnMetadataCache.addAll(columnMetadatas);

				tableMetadatasCache.put(metadata.getParentMetadata().getCode().concat(".").concat(metadata.getCode()),
						metadata);
				// 主键
				genPrimaryKeys(metadata, table, tableMetaModel.getChildMetaModel(primaryKeyMetaModelName),
						columnMetadatas);

				// 外键
				genForeignKeys(metadata, table, tableMetaModel.getChildMetaModel(fkMetaModelName), columnMetadatas);

				// 表分区
				genPartitions(metadata, table, tableMetaModel.getChildMetaModel(pMetaModelName));

				tableMetaModel.addMetadata(metadata);
				if (!tableMetaModel.isHasMetadata()) {
					tableMetaModel.setHasMetadata(true);
				}
			}
		}

		return tableMetadatasCache;
	}
}
