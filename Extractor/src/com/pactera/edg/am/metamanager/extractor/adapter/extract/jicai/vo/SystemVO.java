/**  
 * 文件名：System.java   

*   
* 版本信息：   
* 日期：2012-8-24   
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
* 类名称：System   
* 类描述：   
* 创建人：kaishui   
* 创建时间：2012-8-24 上午10:59:49   
* 修改人：kaishui   
* 修改时间：2012-8-24 上午10:59:49   
* 修改备注：   
* @version    
*    
*/
public class SystemVO implements Serializable {

	/**  
	 * serialVersionUID:TODO（用一句话描述这个变量表示什么）   
	*   
	* @since Ver 1.1   
	*/

	private static final long serialVersionUID = 3673121338846908006L;

	private String sysName;//系统中文名
	private String eSysName;//系统英文名

	private List<SchemaVO> schemas;

	public List<SchemaVO> getSchemas() {
		return schemas;
	}

	public void setSchemas(List<SchemaVO> schemas) {
		this.schemas = schemas;
	}

	public String getSysName() {
		return sysName;
	}

	public void setSysName(String sysName) {
		this.sysName = sysName;
	}

	public String geteSysName() {
		return eSysName;
	}

	public void seteSysName(String eSysName) {
		this.eSysName = eSysName;
	}
}
