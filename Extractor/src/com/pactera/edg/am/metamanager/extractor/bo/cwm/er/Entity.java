package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Clazz;


public class Entity extends Clazz{

	private List<Attribute> attributes;
	
	private List<PrimaryKey> primaryKeies;
	
	private List<ForeignKey> foreignKeies;
	
	// Entity依赖于Domain
	private List<Domain> domains;
	
	private Model ownerModel;

	public Model getOwnerModel() {
		return ownerModel;
	}

	public void setOwnerModel(Model ownerModel) {
		this.ownerModel = ownerModel;
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}

	public List<PrimaryKey> getPrimaryKeies() {
		return primaryKeies;
	}

	public void setPrimaryKeies(List<PrimaryKey> primaryKeies) {
		this.primaryKeies = primaryKeies;
	}

	public List<ForeignKey> getForeignKeies() {
		return foreignKeies;
	}

	public void setForeignKeies(List<ForeignKey> foreignKeies) {
		this.foreignKeies = foreignKeies;
	}

	public List<Domain> getDomains() {
		return domains;
	}

	public void setDomains(List<Domain> domains) {
		this.domains = domains;
	}
}
