package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.CWMPackage;


public class SubjectArea extends CWMPackage{

	// SubjectArea依赖于Entity
	private List<Entity> entities;
	
	private Model ownerModel;

	public Model getOwnerModel() {
		return ownerModel;
	}

	public void setOwnerModel(Model ownerModel) {
		this.ownerModel = ownerModel;
	}

	public List<Entity> getEntities() {
		return entities;
	}

	public void setEntities(List<Entity> entities) {
		this.entities = entities;
	}
}
