package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;


public class ForeignKey extends com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Attribute {

	// ForeignKey依赖于Attribute
	private List<Attribute> attributes;
	
	private Entity ownerEntity;

	public Entity getOwnerEntity() {
		return ownerEntity;
	}

	public void setOwnerEntity(Entity ownerEntity) {
		this.ownerEntity = ownerEntity;
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}
	
}
