package com.pactera.edg.am.metamanager.extractor.bo.cwm.core;

/**
 * 描述模型间的关系，它灵活到足够表达几乎所有元模型间的关系，也正因为这种灵活性，导致它不能保证在此描述的关系都是正确的
 * 注意:此类描述的元数据都为存储库中未存在的元数据
 * @author user
 *
 */
public class Relationship {

	// 关系源
	private ModelElement ownerElement;
	
	// 关系目标
	private ModelElement valueElement;
	
	// 关系的名称
	private String relationshipName;
	
	private String description;

	public ModelElement getOwnerElement() {
		return ownerElement;
	}

	public void setOwnerElement(ModelElement ownerElement) {
		this.ownerElement = ownerElement;
	}

	public ModelElement getValueElement() {
		return valueElement;
	}

	public void setValueElement(ModelElement valueElement) {
		this.valueElement = valueElement;
	}

	public String getRelationshipName() {
		return relationshipName;
	}

	public void setRelationshipName(String relationshipName) {
		this.relationshipName = relationshipName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
