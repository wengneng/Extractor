package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;

/**
 * 该索引依赖于一个表的字段,则创建一个索引对象;如依赖于两个表的字段,则创建两个索引对象...
 * 创建的这么多索引对象,会在mapping处理阶段给予整合
 *
 * @author user
 * @version 1.0  Date: Oct 21, 2010
 *
 */
public class SQLIndex extends ModelElement {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4978010618079249445L;
    
	public static final String INDEX_NAME = "index_name";

	public static final String NON_UNIQUE = "non_unique";

	public static final String ORDINAL_POSITION = "ordinal_position";

	public static final String CARDINALITY = "cardinality";

	public static final String INDEX_TYPE = "index_type";

	public static final String PAGES = "pages";

	private List<IndexedFeature> indexedFeatures;

	// SQLIndex依赖于Table
	private String tableName;

	// 索引同时依赖于字段
	private String columnName;

	// schName.tableName.columnName定位了该索引依赖了对象的位置(注意:当前schema所有的索引,可能作用于其它schema下的表字段)
	private String schName;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public List<IndexedFeature> getIndexedFeatures() {
		return indexedFeatures;
	}

	public void setIndexedFeatures(List<IndexedFeature> indexedFeatures) {
		this.indexedFeatures = indexedFeatures;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;

	}

	public String getTableName() {
		return tableName;
	}

	public void setSchName(String schName) {
		this.schName = schName;
	}
	
	public String getSchName(){
		return schName;
	}
}
