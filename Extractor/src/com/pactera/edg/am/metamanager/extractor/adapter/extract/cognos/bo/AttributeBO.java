package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo;

import java.util.ArrayList;
import java.util.List;

public class AttributeBO {
	// code为xml的标签名
	private List<String> xmlLabelScope = new ArrayList<String>();
	// name为用户看到的名字
	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getXmlLabelScope() {
		return xmlLabelScope;
	}

	public void setXmlLabelScope(List<String> xmlLabelScope) {
		this.xmlLabelScope = xmlLabelScope;
	}
}
