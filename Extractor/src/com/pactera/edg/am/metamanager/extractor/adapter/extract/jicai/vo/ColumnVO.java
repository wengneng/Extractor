/**  
 * 文件名：Field.java   
*   
* 版本信息：   
* 日期：2012-8-24   
* Copyright 足下 Corporation 2012    
* 版权所有   
*   
*/
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo;

/**  
 *    
* 项目名称：metamanager   
* 类名称：Field   
* 类描述：   
* 创建人：kaishui   
* 创建时间：2012-8-24 上午11:21:26   
* 修改人：kaishui   
* 修改时间：2012-8-24 上午11:21:26   
* 修改备注：   
* @version    
*    
*/
public class ColumnVO extends TableVO {

	/**  
	 * serialVersionUID:TODO（用一句话描述这个变量表示什么）   
	*   
	* @since Ver 1.1   
	*/

	private static final long serialVersionUID = -4712537036065496003L;
	private String fieldOrd;//字段序列
	private String fieldName;//字段中文名
	private String eFieldName;//字段英文名
	private String keyFlag;//主键标示
	private String nullFlag;//是否允许为空
	private String fieldType;//字段类型
	private String dbName;//数据库名称
	
	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getFieldOrd() {
		return fieldOrd;
	}

	public void setFieldOrd(String fieldOrd) {
		this.fieldOrd = fieldOrd;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String geteFieldName() {
		return eFieldName;
	}

	public void seteFieldName(String eFieldName) {
		this.eFieldName = eFieldName;
	}

	public String getKeyFlag() {
		return keyFlag;
	}

	public void setKeyFlag(String keyFlag) {
		this.keyFlag = keyFlag;
	}

	public String getNullFlag() {
		return nullFlag;
	}

	public void setNullFlag(String nullFlag) {
		this.nullFlag = nullFlag;
	}

	public String getFieldType() {
		return fieldType;
	}

	public void setFieldType(String fieldType) {
		this.fieldType = fieldType;
	}
}
