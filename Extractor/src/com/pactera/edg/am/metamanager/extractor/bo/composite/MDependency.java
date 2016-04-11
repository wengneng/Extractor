package com.pactera.edg.am.metamanager.extractor.bo.composite;

public class MDependency {

	private MetaModel ownerModel;

	private MetaModel valueModel;

	private String name;

	private String id;

	private String description;

	public MetaModel getOwnerModel() {
		return ownerModel;
	}

	public void setOwnerModel(MetaModel ownerModel) {
		this.ownerModel = ownerModel;
	}

	public MetaModel getValueModel() {
		return valueModel;
	}

	public void setValueModel(MetaModel valueModel) {
		this.valueModel = valueModel;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
