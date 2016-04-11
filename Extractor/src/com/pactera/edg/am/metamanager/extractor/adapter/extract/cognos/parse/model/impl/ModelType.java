package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ModelType {
	// 类型的名称
	private String typeName;
	// 需要解析的标签属性名的集合
	private List<String> attributes=new ArrayList<String>();
	// 路径是否需要加入到表达式中(如folder不需要)
	private boolean isExpression = true;
	//是否解析子孙节点
	private boolean isParseChildern = false;

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<String> attributes) {
		this.attributes = attributes;
	}

	public boolean isExpression() {
		return isExpression;
	}

	public void setExpression(boolean isExpression) {
		this.isExpression = isExpression;
	}

	public boolean isParseChildern() {
		return isParseChildern;
	}

	public void setParseChildern(boolean isParseChildern) {
		this.isParseChildern = isParseChildern;
	}

}
