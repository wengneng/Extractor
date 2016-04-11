package com.pactera.edg.am.metamanager.extractor.bo.mm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;

/**
 * 修改的元数据，记录属性的修改情况
 * 
 * @author user
 * @version 1.0
 * 
 */
public class ModifyMetadata extends MMMetadata {

	/**
	 * 
	 */
	private static final long serialVersionUID = 785520992L;

	/**
	 * 增加，删除,没变的属性
	 */
	private Map<Operation, Map<String, String>> modifyAttrs = new HashMap<Operation, Map<String, String>>(0);

	private List<ModifyAttribute> mAttrs = new ArrayList<ModifyAttribute>(0);

	public List<ModifyAttribute> getMAttrs() {
		return mAttrs;
	}

	public void setMAttrs(List<ModifyAttribute> attrs) {
		mAttrs = attrs;
	}

	public Map<Operation, Map<String, String>> getModifyAttrs() {
		return modifyAttrs;
	}

	public void setModifyAttrs(Map<Operation, Map<String, String>> modifyAttrs) {
		this.modifyAttrs = modifyAttrs;
	}

}