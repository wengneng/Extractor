package com.pactera.edg.am.metamanager.extractor.bo.cwm.er;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.CWMPackage;


public class Model extends CWMPackage{

	private List<Domain> domains;

	private List<Entity> entities;

	private List<Relationship> relationships;

	private List<SubjectArea> subjectAreas;

	private List<NonuniqueConstraint> NonuniqueKeies;
	
	private ModelLibrary ownerModelLibrary;

	public ModelLibrary getOwnerModelLibrary() {
		return ownerModelLibrary;
	}

	public void setOwnerModelLibrary(ModelLibrary ownerModelLibrary) {
		this.ownerModelLibrary = ownerModelLibrary;
	}

	public List<Domain> getDomains() {
		return domains;
	}

	public void setDomains(List<Domain> domains) {
		this.domains = domains;
	}

	public List<Entity> getEntities() {
		return entities;
	}

	public void setEntities(List<Entity> entities) {
		this.entities = entities;
	}

	public List<Relationship> getRelationships() {
		return relationships;
	}

	public void setRelationships(List<Relationship> relationships) {
		this.relationships = relationships;
	}

	public List<SubjectArea> getSubjectAreas() {
		return subjectAreas;
	}

	public void setSubjectAreas(List<SubjectArea> subjectAreas) {
		this.subjectAreas = subjectAreas;
	}

	public List<NonuniqueConstraint> getNonuniqueKeies() {
		return NonuniqueKeies;
	}

	public void setNonuniqueKeies(List<NonuniqueConstraint> nonuniqueKeies) {
		NonuniqueKeies = nonuniqueKeies;
	}
}
