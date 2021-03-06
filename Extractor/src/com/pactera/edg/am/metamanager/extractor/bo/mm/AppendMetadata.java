/*
 * Copyright 2009 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, BeiJing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

package com.pactera.edg.am.metamanager.extractor.bo.mm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 悬挂结点元数据
 * 
 * @author user
 * @version 1.0 Date: Sep 29, 2009
 * 
 */
public class AppendMetadata implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 335874598975L;

	/**
	 * 属于该悬挂结点的模型的子模型(包含元数据)
	 */
	private Set<MMMetaModel> childMetaModels = new HashSet<MMMetaModel>(2);

	/**
	 * 属于该悬挂结点的所有依赖关系
	 */
	private List<MMDDependency> dDependencies = new ArrayList<MMDDependency>(0);

	private MMMetadata metadata;

	private MMMetaModel metaModel;

	private Map<String, List<Map<String, String>>> deleteMapDatas = new HashMap<String, List<Map<String, String>>>(0);

	public Map<String, List<Map<String, String>>> getDeleteMapDatas() {
		return deleteMapDatas;
	}

	public void setDeleteMapDatas(Map<String, List<Map<String, String>>> deleteMapDatas) {
		this.deleteMapDatas = deleteMapDatas;
	}

	public MMMetaModel getMetaModel() {
		return metaModel;
	}

	public void setMetaModel(MMMetaModel metaModel) {
		this.metaModel = metaModel;
	}

	public Set<MMMetaModel> getChildMetaModels() {
		return childMetaModels;
	}

	public MMMetaModel getChildMetaModel(String classifierId) {
		for (MMMetaModel mModel : childMetaModels) {
			if (mModel.getName().equals(classifierId))
				return mModel;
		}
		return null;
	}

	public void setChildMetaModels(Set<MMMetaModel> childMetaModels) {
		this.childMetaModels = childMetaModels;
	}

	public List<MMDDependency> getDDependencies() {
		return dDependencies;
	}

	public void setDDependencies(List<MMDDependency> dependencies) {
		dDependencies = dependencies;
	}

	public boolean clearCache() {
		childMetaModels.clear();
		dDependencies.clear();
		metaModel.getChildMetaModels().clear();
		metadata.getChildrenMetadatas().clear();

		return true;
	}

	public AppendMetadata cloneAppendMetadata() {
		AppendMetadata aMetadata = new AppendMetadata();
		aMetadata.setMetadata(metadata);
		aMetadata.setMetaModel(metaModel);
		return aMetadata;
	}

	public MMMetadata getMetadata() {
		return metadata;
	}

	public void setMetadata(MMMetadata metadata) {
		this.metadata = metadata;
	}

}
