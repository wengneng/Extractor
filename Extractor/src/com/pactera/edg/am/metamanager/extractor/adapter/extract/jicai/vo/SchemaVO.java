/**  
 * 文件名：SchemaVO.java   
*   
* 版本信息：   
* 日期：2012-8-27   
* Copyright 足下 Corporation 2012    
* 版权所有   
*   
*/
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo;

import java.io.Serializable;
import java.util.List;

/**  
 *    
* 项目名称：metamanager   
* 类名称：SchemaVO   
* 类描述：   
* 创建人：kaishui   
* 创建时间：2012-8-27 上午10:15:00   
* 修改人：kaishui   
* 修改时间：2012-8-27 上午10:15:00   
* 修改备注：   
* @version    
*    
*/
public class SchemaVO implements Serializable {

	/**  
	 * serialVersionUID:TODO（用一句话描述这个变量表示什么）   
	*   
	* @since Ver 1.1   
	*/

	private static final long serialVersionUID = 5698871660436640541L;
	private String schName;
	private List<ColumnVO> fields;
	private List<TableVO> tables;

	public String getSchName() {
		return schName;
	}

	public void setSchName(String schName) {
		this.schName = schName;
	}

	public List<ColumnVO> getFields() {
		return fields;
	}

 	public void setFields(List<ColumnVO> fields) {
		this.fields = fields;
	}

	public List<TableVO> getTables() {
		return tables;
	}

	public void setTables(List<TableVO> tables) {
		this.tables = tables;
	}
}
