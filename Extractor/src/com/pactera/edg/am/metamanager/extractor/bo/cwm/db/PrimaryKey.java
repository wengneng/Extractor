package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Attribute;

/**
 * 该主键依赖于一个表的字段,则创建一个主键对象;如依赖于两个表的字段,则创建两个主键对象...
 * 创建的这么多主键对象,会在mapping处理阶段给予整合
 *
 * @author user
 * @version 1.0  Date: Oct 21, 2010
 *
 */
public class PrimaryKey extends Attribute {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6024875214963046628L;

	// PrimaryKey
	public final static String PK_NAME = "PK_NAME";

	public static final String KEY_SEQ = "key_seq";

	// PrimaryKey依赖于TableColumn
	private String columnName;

	private Table ownerTable;

	public Table getOwnerTable() {
		return ownerTable;
	}

	public void setOwnerTable(Table ownerTable) {
		this.ownerTable = ownerTable;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

}
