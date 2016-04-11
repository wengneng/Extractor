package com.pactera.edg.am.metamanager.extractor.bo.composite;

import java.util.ArrayList;
import java.util.List;

public class MetaModel {

	private List<MetaModel> mmComponents = new ArrayList<MetaModel>();

	private String name;

	private String description;

	private String id;

	private List<MAttribute> attrs;

	public void add(MetaModel mm) {
		mmComponents.add(mm);
	}

	public List<MetaModel> getMmComponents() {
		return mmComponents;
	}

	public void setMmComponents(List<MetaModel> mmComponents) {
		this.mmComponents = mmComponents;
	}

	public List<MAttribute> getAttrs() {
		return attrs;
	}

	public void setAttrs(List<MAttribute> attrs) {
		this.attrs = attrs;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void remove(MetaModel mm) {
		mmComponents.remove(mm);
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
