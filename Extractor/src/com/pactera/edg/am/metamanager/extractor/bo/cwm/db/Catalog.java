package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;


public class Catalog extends ModelElement{

	private List<Schema> schemas;

	public List<Schema> getSchemas() {
		return schemas;
	}

	public void setSchemas(List<Schema> schemas) {
		this.schemas = schemas;
	}
}
