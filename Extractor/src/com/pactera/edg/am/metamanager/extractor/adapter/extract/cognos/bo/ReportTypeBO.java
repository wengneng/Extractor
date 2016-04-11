package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo;

import java.util.ArrayList;
import java.util.List;

public class ReportTypeBO {
	// code为xml的标签名
	private String xmlLabel;
	// name为用户看到的名字
	private String name;
	private List<AttributeBO> attributes = new ArrayList<AttributeBO>();

	public List<AttributeBO> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<AttributeBO> attributes) {
		this.attributes = attributes;
	}

	public String getXmlLabel() {
		return xmlLabel;
	}

	public void setXmlLabel(String xmlLabel) {
		this.xmlLabel = xmlLabel;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
