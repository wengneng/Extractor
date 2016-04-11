package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Constraint;


/**
 * 索引
 * 
 * @author user
 * 
 */
public class NonuniqueConstraint extends Constraint {

	// NonuniqueKey依赖于Entity
	private List<Entity> entities;

	private List<IndexedFeature> indexedFeatures;
	
	private Model ownerModel;

	public Model getOwnerModel() {
		return ownerModel;
	}

	public void setOwnerModel(Model ownerModel) {
		this.ownerModel = ownerModel;
	}

	public List<IndexedFeature> getIndexedFeatures() {
		return indexedFeatures;
	}

	public void setIndexedFeatures(List<IndexedFeature> indexedFeatures) {
		this.indexedFeatures = indexedFeatures;
	}

	public List<Entity> getEntities() {
		return entities;
	}

	public void setEntities(List<Entity> entities) {
		this.entities = entities;
	}

}
