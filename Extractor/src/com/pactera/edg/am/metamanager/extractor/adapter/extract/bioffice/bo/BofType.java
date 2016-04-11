package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo;

import java.util.ArrayList;
import java.util.List;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;

public class BofType {
	private String name;
	private String modelName;
	private Class parseClass = AbstractParse.class;
	private boolean isRoot = false;
	private boolean isFieldLevel = false;
	private List<String> dependencys = new ArrayList<String>();

	public List<String> getDependencys() {
		return dependencys;
	}

	public void setDependencys(List<String> dependencys) {
		this.dependencys = dependencys;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getModelName() {
		return modelName;
	}

	public void setModelName(String modelName) {
		this.modelName = modelName;
	}

	public boolean isRoot() {
		return isRoot;
	}

	public void setRoot(boolean isRoot) {
		this.isRoot = isRoot;
	}

	public Class getParseClass() {
		return parseClass;
	}

	public void setParseClass(Class parseClass) {
		this.parseClass = parseClass;
	}

	public boolean isFieldLevel() {
		return isFieldLevel;
	}

	public void setFieldLevel(boolean isFieldLevel) {
		this.isFieldLevel = isFieldLevel;
	}
}
