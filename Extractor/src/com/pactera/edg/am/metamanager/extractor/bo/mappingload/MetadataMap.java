package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 全量入库使用的对象:元数据及属性，元数据的关系
 * 
 * @author user
 * @version 1.0
 * 
 */
public class MetadataMap implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3247895709809838L;

	/**
	 * Map中的operation是指操作的类型，包括添加，删除，List是指需要操作的元数据
	 */
	private Map<Operation, List<Metadata>> adapterMetadatas;

	/**
	 * 指需要添加，删除的元数据关系，这里指依赖关系
	 */
	private Map<Operation, List<DRelationship>> adapterRelationship;

	public Map<Operation, List<Metadata>> getAddDelMetadatas() {
		return adapterMetadatas;
	}

	public void setAdapterMetadatas(
			Map<Operation, List<Metadata>> adapterMetadatas) {
		this.adapterMetadatas = adapterMetadatas;
	}

	public Map<Operation, List<DRelationship>> getdRelationship() {
		return adapterRelationship;
	}

	public void setAdapterRelationship(
			Map<Operation, List<DRelationship>> adapterRelationship) {
		this.adapterRelationship = adapterRelationship;
	}

}
