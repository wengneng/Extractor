package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

public class Attribute extends com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Attribute {

	// Attribute依赖于Domain
	private List<Domain> domains;
	
	private Entity ownerEntity;

	public Entity getOwnerEntity() {
		return ownerEntity;
	}

	public void setOwnerEntity(Entity ownerEntity) {
		this.ownerEntity = ownerEntity;
	}

	public List<Domain> getDomains() {
		return domains;
	}

	public void setDomains(List<Domain> domains) {
		this.domains = domains;
	}

}
