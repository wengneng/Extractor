package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;


public class IndexedFeature extends ModelElement {

	// IndexedFeature 索引项依赖于Attribute
	private List<Attribute> attributes;
	
	private NonuniqueConstraint ownerConstraint;

	public NonuniqueConstraint getOwnerConstraint() {
		return ownerConstraint;
	}

	public void setOwnerConstraint(NonuniqueConstraint ownerConstraint) {
		this.ownerConstraint = ownerConstraint;
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}
}
