package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

import java.util.List;

/**
 * 增量入库的对象
 * 
 * @author user
 * @version 1.0
 * 
 */
public final class DiffMetadataMap extends MetadataMap {

	/**
	 * 
	 */
	private static final long serialVersionUID = 23785404398L;
	/**
	 * 修改的元数据，此处只指属性的修改，包括对属性的添加，修改，删除
	 */
	private List<ModifyMetadata> modifyMetadatas;

	public List<ModifyMetadata> getModifyMetadatas() {
		return modifyMetadatas;
	}

	public void setModifyMetadatas(List<ModifyMetadata> modifyMetadatas) {
		this.modifyMetadatas = modifyMetadatas;
	}

}
