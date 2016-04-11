package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

import java.util.Map;

/**
 * 修改的元数据，记录属性的修改情况
 * @author user
 * @version 1.0
 *
 */
public class ModifyMetadata extends Metadata {

	/**
	 * 
	 */
	private static final long serialVersionUID = 785520992L;
	
	/**
	 * 增加，删除的属性，将修改的属性也放在增加里头
	 */
	private Map<Operation, Map<String, String>> modifyAttrs;

	public Map<Operation, Map<String, String>> getModifyAttrs() {
		return modifyAttrs;
	}

	public void setModifyAttrs(Map<Operation, Map<String, String>> modifyAttrs) {
		this.modifyAttrs = modifyAttrs;
	}

}