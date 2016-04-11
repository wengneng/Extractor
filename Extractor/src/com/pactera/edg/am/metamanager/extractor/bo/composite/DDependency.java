package com.pactera.edg.am.metamanager.extractor.bo.composite;

/**
 * 描述元数据间的依赖关系
 * 
 * @author user
 * 
 */
public class DDependency {

	private CompMetadata ownerMetadata;
	
	// 所属的元模型
	private MDependency mDependency;

	private CompMetadata valueMetadata;

	private String name;

	private String description;

	public CompMetadata getOwnerMetadata() {
		return ownerMetadata;
	}

	public void setOwnerMetadata(CompMetadata ownerMetadata) {
		this.ownerMetadata = ownerMetadata;
	}

	public CompMetadata getValueMetadata() {
		return valueMetadata;
	}

	public void setValueMetadata(CompMetadata valueMetadata) {
		this.valueMetadata = valueMetadata;
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

	public MDependency getMDependency() {
		return mDependency;
	}

	public void setMDependency(MDependency dependency) {
		mDependency = dependency;
	}

}
