package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.parse.AbstractParse;

public class PcType {
	private String name;
	private Class parseClass = AbstractParse.class;
	private boolean isParseDependency=false;
	public boolean isParseDependency() {
		return isParseDependency;
	}
	public void setParseDependency(boolean isParseDependency) {
		this.isParseDependency = isParseDependency;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Class getParseClass() {
		return parseClass;
	}
	public void setParseClass(Class parseClass) {
		this.parseClass = parseClass;
	}
}
