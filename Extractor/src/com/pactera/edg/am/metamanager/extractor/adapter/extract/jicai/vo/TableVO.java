/**  
 * 文件名：Table.java   
*   
* 版本信息：   
* 日期：2012-8-24   
* Copyright 足下 Corporation 2012    
* 版权所有   
*   
*/
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo;

import java.util.List;

/**  
 *    
* 项目名称：metamanager   
* 类名称：Table   
* 类描述：   
* 创建人：kaishui   
* 创建时间：2012-8-24 上午11:20:52   
* 修改人：kaishui   
* 修改时间：2012-8-24 上午11:20:52   
* 修改备注：   
* @version    
*    
*/
public class TableVO extends SystemVO {

	/**  
	 * serialVersionUID:TODO（用一句话描述这个变量表示什么）   
	*   
	* @since Ver 1.1   
	*/

	private static final long serialVersionUID = 9195882821818895987L;
	private String dbName;//数据库名
	private String tableName;//表中文名
	private String eTableName;//表英文名
	private String desc;//描述
	private String tableType;//表类型
	private String isPk;//是否存在主键
	private String uniqueIndexName;//唯一索引

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String geteTableName() {
		return eTableName;
	}

	public void seteTableName(String eTableName) {
		this.eTableName = eTableName;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public String getIsPk() {
		return isPk;
	}

	public void setIsPk(String isPk) {
		this.isPk = isPk;
	}

	public String getUniqueIndexName() {
		return uniqueIndexName;
	}

	public void setUniqueIndexName(String uniqueIndexName) {
		this.uniqueIndexName = uniqueIndexName;
	}

	public String getNonuniqueIndexName() {
		return nonuniqueIndexName;
	}

	public void setNonuniqueIndexName(String nonuniqueIndexName) {
		this.nonuniqueIndexName = nonuniqueIndexName;
	}

	private String nonuniqueIndexName;//非唯一索引

}
