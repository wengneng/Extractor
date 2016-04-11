package com.pactera.edg.am.metamanager.extractor.bo.cwm.core;

import java.util.HashMap;
import java.util.Map;

public class ModelElement extends Element {
	public final static String REMARKS = "remarks";

	private String name;

	/**
	 * 元数据的属性:key-->value
	 */
	private Map<String, String> attrs = new HashMap<String, String>(0);

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getAttrs() {
		return attrs;
	}

	public void addAttr(String key, String value) {
		if (value != null && !value.equals("")) {
			attrs.put(key, value);
		}
	}

}
