package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Classifier;

public class Domain extends Classifier{

	private Model ownerModel;

	public Model getOwnerModel() {
		return ownerModel;
	}

	public void setOwnerModel(Model ownerModel) {
		this.ownerModel = ownerModel;
	}
}
