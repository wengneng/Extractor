package com.pactera.edg.am.metamanager.extractor.bo.cwm.db;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Clazz;

public abstract class ColumnSet extends Clazz{

	private Schema ownerSchema;

	public Schema getOwnerSchema() {
		return ownerSchema;
	}

	public void setOwnerSchema(Schema ownerSchema) {
		this.ownerSchema = ownerSchema;
	}
	
}
