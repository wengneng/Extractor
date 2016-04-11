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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 元模型,它包含属性该元模型的元数据,同时子孙元模型也将在此
 * 
 * @author user
 * @version 1.0 Date: Jul 27, 2009
 * 
 */
public class MMMetaModel implements Serializable {

	private Log log = LogFactory.getLog(MMMetaModel.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 3434465476578L;

	private Set<MMMetaModel> childMetaModels = new HashSet<MMMetaModel>(2);

	/**
	 * 是否有子元模型,如果childMetaModels不为空,则hasChildMetaModel为TRUE,否则为FALSE
	 */
	private boolean hasChildMetaModel;

	private String name;

	private String description;

	private String code;

	/**
	 * 该元模型与其父元模型间的组合关系代码
	 */
	private String compedRelationCode;

	/**
	 * 元模型的属性,key:元模型名;value:元模型在库表中的位置
	 */
	private Map<String, String> mAttrs;
	
	/**
	 * 需作变更比较的元数据属性列表:与haveChangelessAttr合用;如果这个列表中有一个及以上个属性
	 * 则表示列表中的属性为需要作变更比较的属性,而不在列表之内的属性,将不作变更比较;注意这个列表的size=0时,表示所有属性都不需要变更比较.
	 */
	private Set<String> needUpdateAttrs = new HashSet<String>(0);
	
	/**
	 * 是否有不需要变更的属性的标志,默认为false表示没有,即所有属性都参与作变更比较;如果有,则置状态为true,
	 * 此时把需要变更的属性放在needUpdateAttrs中,不在此列的即为不需要变更的
	 */
	private boolean haveChangelessAttr = false;

	private MMMetaModel parentMetaModel;

	private List<AbstractMetadata> metadatas = new ArrayList<AbstractMetadata>();

	/**
	 * 是否有属于该元模型的元数据,如果metadatas不为空,则hasMetadata为TRUE,否则为FALSE
	 */
	private boolean hasMetadata;

	/**
	 * 命名空间
	 */
	private String namespace;

	// hashCode
	private int hashCode;
	
	public Set<String> getNeedUpdateAttrs(){
		return needUpdateAttrs;
	}
	
	public void addNeedUpdateAttr(String attr){
		needUpdateAttrs.add(attr);
	}


	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public Set<MMMetaModel> getChildMetaModels() {
		return childMetaModels;
	}

	public void setChildMetaModels(Set<MMMetaModel> childMetaModels) {
		this.childMetaModels = childMetaModels;
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

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public List<AbstractMetadata> getMetadatas() {
		return metadatas;
	}

	public boolean addMetadata(MMMetadata metadata) {
		return metadatas.add(metadata);
	}

	public boolean addMetadatas(List<MMMetadata> metadatas) {
		return this.metadatas.addAll(metadatas);
	}

	public boolean addMetaModel(MMMetaModel metaModel) {
		return childMetaModels.add(metaModel);
	}

	public MMMetaModel getChildMetaModel(String childCode) {
		for (MMMetaModel metaModel : childMetaModels) {
			if (childCode.equals(metaModel.getCode())) { return metaModel; }
		}

		// 没有找到
		return null;
	}

	public void setMetadatas(List<AbstractMetadata> metadatas) {
		this.metadatas = metadatas;
	}

	public boolean isHasChildMetaModel() {
		if(childMetaModels.size() > 0){
			return true;
		}
		return false;
	}

	public void setHasChildMetaModel(boolean hasChildMetaModel) {
		this.hasChildMetaModel = hasChildMetaModel;
	}

	public boolean isHasMetadata() {
		if(metadatas.size() > 0){
			return true;
		}
		return false;
	}

	public void setHasMetadata(boolean hasMetadata) {
		this.hasMetadata = hasMetadata;
	}

	public MMMetaModel getParentMetaModel() {
		return parentMetaModel;
	}

	public void setParentMetaModel(MMMetaModel parentMetaModel) {
		this.parentMetaModel = parentMetaModel;
	}

	public MMMetaModel cloneOnlyMetaModel() {
		MMMetaModel cloneMetaModel = new MMMetaModel();
		cloneMetaModel.setCode(code);
		cloneMetaModel.setName(name);
		cloneMetaModel.setNamespace(namespace);
		cloneMetaModel.setParentMetaModel(parentMetaModel);
		cloneMetaModel.setMAttrs(mAttrs);
		return cloneMetaModel;

	}

	/**
	 * 比较两元模型是否相同:如两者CODE相同,则认为两元模型相同
	 * 
	 * @param o
	 * @return
	 */
	public boolean equals(Object o) {
		if (!(o instanceof MMMetaModel)) { return false; }
		MMMetaModel metaModel = (MMMetaModel) o;
		return (code == metaModel.getCode() || (code != null && code.equals(metaModel.getCode())));
	}

	public int hashCode() {
		if (hashCode == 0)
			return 17 + 37 * code.hashCode();
		return hashCode;
	}

	/**
	 * 设置元模型的所有子元模型
	 * 
	 * @param childMetaModelNames
	 */
	public void addChildMetaModels(String childMetaModelName1, String childMetaModelName2, String childMetaModelName3,
			String... childMetaModelNames) {
		addChildMetaModels(childMetaModelName1, childMetaModelName2, childMetaModelName3);
		for (int i = 0; i < childMetaModelNames.length; i++) {
			addChildMetaModels(childMetaModelNames[i]);
		}
	}

	/**
	 * 设置元模型的所有子元模型
	 * 
	 * @param childMetaModelNames
	 */
	public void addChildMetaModels(String childMetaModelName1) {
		for (Iterator<MMMetaModel> iter = childMetaModels.iterator(); iter.hasNext();) {
			MMMetaModel childMetaModel = iter.next();
			if (childMetaModel.getCode().equals(childMetaModelName1)) {
				// 元模型已经存在该模型
				return;
			}
		}

		addChildMetaModel(childMetaModelName1);

		setHasChildMetaModel(true);
	}

	/**
	 * 设置元模型的所有子元模型
	 * 
	 * @param childMetaModelNames
	 */
	public void addChildMetaModels(String childMetaModelName1, String childMetaModelName2) {
		boolean hasChild1 = false, hasChild2 = false;
		for (Iterator<MMMetaModel> iter = childMetaModels.iterator(); iter.hasNext();) {
			MMMetaModel childMetaModel = iter.next();
			if (childMetaModel.getCode().equals(childMetaModelName1)) {
				// 元模型已经存在该模型
				hasChild1 = true;
			}
			else if (childMetaModel.getCode().equals(childMetaModelName2)) {
				hasChild2 = true;
			}
		}
		if (!hasChild1) {
			addChildMetaModel(childMetaModelName1);
		}
		if (!hasChild2) {
			addChildMetaModel(childMetaModelName2);
		}

		setHasChildMetaModel(true);
	}

	/**
	 * 设置元模型的所有子元模型
	 * 
	 * @param childMetaModelNames
	 */
	public void addChildMetaModels(String childMetaModelName1, String childMetaModelName2, String childMetaModelName3) {
		boolean hasChild1 = false, hasChild2 = false, hasChild3 = false;
		for (Iterator<MMMetaModel> iter = childMetaModels.iterator(); iter.hasNext();) {
			MMMetaModel childMetaModel = iter.next();
			if (childMetaModel.getCode().equals(childMetaModelName1)) {
				// 元模型已经存在该模型
				hasChild1 = true;
			}
			else if (childMetaModel.getCode().equals(childMetaModelName2)) {
				hasChild2 = true;
			}
			else if (childMetaModel.getCode().equals(childMetaModelName3)) {
				hasChild3 = true;
			}
		}
		if (!hasChild1) {
			addChildMetaModel(childMetaModelName1);
		}
		if (!hasChild2) {
			addChildMetaModel(childMetaModelName2);
		}
		if (!hasChild3) {
			addChildMetaModel(childMetaModelName3);
		}

		setHasChildMetaModel(true);
	}

	public MMMetaModel addChildMetaModel(String childMetaModelName) {
		MMMetaModel childMetaModel = new MMMetaModel();
		childMetaModel.setName(childMetaModelName);
		childMetaModel.setCode(childMetaModelName);
		childMetaModel.setParentMetaModel(this);
		addMetaModel(childMetaModel);
		return childMetaModel;
	}

	public void print() {
		log.info("MetaModel:" + name);
		log.info("code:" + code);
		if (metadatas.size() > 0)
			for (AbstractMetadata metadata : metadatas) {
				metadata.print();
			}

		if (childMetaModels.size() > 0) {
			for (MMMetaModel childMetaModel : childMetaModels) {
				childMetaModel.print();
			}
		}
	}

	public String getCompedRelationCode() {
		return compedRelationCode;
	}

	public void setCompedRelationCode(String compedRelationCode) {
		this.compedRelationCode = compedRelationCode;
	}

	public Map<String, String> getMAttrs() {
		if(mAttrs == null){
			return Collections.emptyMap();//new HashMap<String, String>(0);
		}
		return mAttrs;
	}

	public void setMAttrs(Map<String, String> attrs) {
		mAttrs = attrs;
	}

	public boolean isHaveChangelessAttr() {
		return haveChangelessAttr;
	}

	public void setHaveChangelessAttr(boolean haveChangelessAttr) {
		this.haveChangelessAttr = haveChangelessAttr;
	}
}
