package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

/**
 * 元模型关系类 
 *
 * @author user
 * @version 1.0  Date: Jul 7, 2009
 *
 */
public class MRelationship {

	// 关系的源着一端元数据
	private MetaModel ownerMetaModel;

	// 关系的目标一端元数据
	private MetaModel valueMetaModel;

	// 关系名
	private String name;

	private String description;

	// 关系类型:1为composition合成，2为dependency依赖，3为association非合成的关联关系
	private byte type;

	public MetaModel getOwnerMetaModel() {
		return ownerMetaModel;
	}

	public void setOwnerMetaModel(MetaModel ownerMetaModel) {
		this.ownerMetaModel = ownerMetaModel;
	}

	public MetaModel getValueMetaModel() {
		return valueMetaModel;
	}

	public void setValueMetaModel(MetaModel valueMetaModel) {
		this.valueMetaModel = valueMetaModel;
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

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

}
