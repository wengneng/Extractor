package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.Subsystem;


public class ModelLibrary extends Subsystem {

	private List<ModelLibrary> modelLibraries;

	private ModelLibrary ownerModelLibrary;

	private List<Model> models;

	public ModelLibrary getOwnerModelLibrary() {
		return ownerModelLibrary;
	}

	public void setOwnerModelLibrary(ModelLibrary ownerModelLibrary) {
		this.ownerModelLibrary = ownerModelLibrary;
	}

	public List<ModelLibrary> getModelLibraries() {
		return modelLibraries;
	}

	public void setModelLibraries(List<ModelLibrary> modelLibraries) {
		this.modelLibraries = modelLibraries;
	}

	public List<Model> getModels() {
		return models;
	}

	public void setModels(List<Model> models) {
		this.models = models;
	}
}
