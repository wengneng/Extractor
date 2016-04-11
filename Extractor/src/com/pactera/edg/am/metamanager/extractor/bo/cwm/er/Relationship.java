package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Clazz;


public class Relationship extends Clazz{

	private Model ownerModel;

	public Model getOwnerModel() {
		return ownerModel;
	}

	public void setOwnerModel(Model ownerModel) {
		this.ownerModel = ownerModel;
	}
	
	private List<RelationshipEnd> relationshipEnds;

	public List<RelationshipEnd> getRelationshipEnds() {
		return relationshipEnds;
	}

	public void setRelationshipEnds(List<RelationshipEnd> relationshipEnds) {
		this.relationshipEnds = relationshipEnds;
	}
}
