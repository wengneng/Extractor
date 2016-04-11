/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, BeiJing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */


package com.pactera.edg.am.metamanager.extractor.increment;

import java.util.List;
import java.util.Set;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.ex.SpringContextLoadException;

/**
 * 根据已存在于存储库中的元数据ID,从库表中获取其所有子孙结点的信息
 *
 * @author user
 * @version 1.0  Date: Jul 31, 2009
 *
 */
public interface IGenMetadataService {

	/**
	 * 根据已存在于存储库中的元数据ID,从库表中获取其所有子孙结点的信息
	 * @param metadataId 元数据ID
	 * @return 顶级结点的元模型(其包含属性该模型的元数据,及其所有子孙元模型和属于子孙元模型的元数据)
	 */
	MMMetaModel genMetadatas(MMMetadata parentMetadata, String metadataId) throws SpringContextLoadException;

	/**
	 * 判断该元数据结点是否已经存在于元数据库中
	 * @param parentId 元数据的父结点ID
	 * @param rootMetadata 待判断的元数据
	 * @return 如已经存在则返回true, 否则返回false
	 */
	boolean existMetadata(String parentId, MMMetadata rootMetadata);

	/**
	 * 获取元模型属于metaModel,且父结点为parentMetadataIds列表的元数据列表
	 * @param parentMetadataIds
	 * @param metaModel
	 * @return
	 */
	List<AbstractMetadata> genChildMetadatas(List<AbstractMetadata> parentMetadatas, MMMetaModel metaModel);

	/**
	 * 根据元数据命名空间,获取其所有子孙结点的依赖关系
	 * @param aNamespace
	 * @return 子孙结点依赖关系列表
	 */
	List<MMDDependency> genDbDependencies(String aNamespace);

	/**
	 * 根据元数据命名空间,获取其直接子结点的ID列表
	 * @param namespace
	 * @return 直接子结点的ID列表
	 */
	List<MMMetadata> genChildrenMetadatasId(String namespace);

	/**
	 * 根据元数据namespace列表,获取其子孙元数据的依赖关系
	 * @param metadataIds
	 * @return
	 */
	List<MMDDependency> genDbDependencies(Set<String> metadataNamespaces);
	
	/**
	  * 根据当前节点ID获取父节点ID
	  * @Title: getParId  
	  * @Description: TODO 
	  * @param @param id
	  * @param @return 
	  * @return String 
	  * @throws
	 */
	String getParId(String id);
	
}
