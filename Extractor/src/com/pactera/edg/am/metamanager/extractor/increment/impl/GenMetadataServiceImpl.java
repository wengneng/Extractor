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

package com.pactera.edg.am.metamanager.extractor.increment.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.IDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetaModelDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao;
import com.pactera.edg.am.metamanager.extractor.ex.MetadataNotFoundException;
import com.pactera.edg.am.metamanager.extractor.ex.SpringContextLoadException;
import com.pactera.edg.am.metamanager.extractor.increment.IGenMetadataService;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 从库表中获取数据(包括获取一个元数据的子孙结点的信息)
 * 
 * @author user
 * @version 1.0 Date: Jul 31, 2009
 * 
 */
public class GenMetadataServiceImpl implements IGenMetadataService {

	private Log log = LogFactory.getLog(GenMetadataServiceImpl.class);

	/**
	 * 存储库中的元数据的DAO接口
	 */
	private IMetadataDao metadataDao;

	/**
	 * 存储库中的元模型的DAO接口
	 */
	private IMetaModelDao metaModelDao;
	
	private IDependencyDao dependencyDao;

	private int count;

	public void setMetaModelDao(IMetaModelDao metaModelDao) {
		this.metaModelDao = metaModelDao;
	}

	public void setMetadataDao(IMetadataDao metadataDao) {
		this.metadataDao = metadataDao;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.increment.IGenMetadataService#genMetadatas(String)
	 */
	public MMMetaModel genMetadatas(MMMetadata parentMetadata, String metadataId) throws SpringContextLoadException {
		// 1.根据元数据ID获取其元模型
		count = 0;
		MMMetaModel metaModel = queryMetaModelBySql(metadataId);
		if (metaModel == null) {
			log.error(new StringBuilder("需要作增量比较的数据[").append(metadataId).append("]在元数据存储库中不存在!").toString());
			return null;

		}
		MMMetadata metadata = genMetadata(parentMetadata, metadataId, metaModel);
		metaModel.addMetadata(metadata);
		metaModel.setHasMetadata(true);
		// 2.递归返回所有元模型及元数据
		count++;
		genChildMetaModels(metaModel, metadata);

		log.info("Metadatas size:" + count);
		return metaModel;
	}

	/**
	 * 根据元数据ID及其所属的元模型信息,获取该元数据的详细信息
	 * 
	 * @param metadataId
	 * @param metaModel
	 * @return 元数据详细信息
	 */
	private MMMetadata genMetadata(MMMetadata parentMetadata, String metadataId, MMMetaModel metaModel) {

		return metadataDao.queryMetadata(parentMetadata, metadataId, metaModel);

	}

	/**
	 * 递归获取模型的所有子模型及所含该模型的子结点
	 * 
	 * @param metaModel
	 * @param metadata
	 * @return
	 */
	private void genChildMetaModels(MMMetaModel metaModel, MMMetadata metadata) {
		// 获取元模型的所有子元模型
		List<MMMetaModel> childMetaModels = metaModelDao.queryChileMetaModel(metaModel.getCode());
		if (childMetaModels.size() <= 0) { return; }

		for (MMMetaModel childMetaModel : childMetaModels) {
			genChildMetaModel(childMetaModel, metadata);
			if (childMetaModel.isHasMetadata()) {
				// 有元数据的,才添加进来
				metaModel.addMetaModel(childMetaModel);
				// 子元模型不为空
				metaModel.setHasChildMetaModel(true);
			}

		}
	}

	private void genChildMetaModel(MMMetaModel metaModel, MMMetadata parentMetadata) {
		List<AbstractMetadata> metadatas = metadataDao.queryChildMetadatas(parentMetadata, metaModel);
		// 设置子元模型的所有元数据
		if (metadatas.size() > 0) {
			count += metadatas.size();
			metaModel.setMetadatas(metadatas);
			metaModel.setHasMetadata(true);

			genChildMetaModels(metaModel, metadatas);
		}

	}

	/**
	 * 递归获取模型的所有子模型及所含该模型的子结点
	 * 
	 * @param metaModel
	 * @param metadata
	 * @return
	 */
	private void genChildMetaModels(MMMetaModel metaModel, List<AbstractMetadata> parentMetadatas) {
		// 获取元模型的所有子元模型
		List<MMMetaModel> childMetaModels = metaModelDao.queryChileMetaModel(metaModel.getCode());
		if (childMetaModels.size() <= 0) { return; }
		for (MMMetaModel childMetaModel : childMetaModels) {
			List<AbstractMetadata> metadatas = genChildMetadatas(parentMetadatas, childMetaModel);

			if (metadatas.size() > 0) {
				count += metadatas.size();
				childMetaModel.setMetadatas(metadatas);
				childMetaModel.setHasMetadata(true);
				genChildMetaModels(childMetaModel, metadatas);
				// 有元数据,才添加该元模型
				metaModel.addMetaModel(childMetaModel);
				// 子元模型不为空
				metaModel.setHasChildMetaModel(true);

			}
		}

	}

	/**
	 * 根据元数据ID获取其元模型
	 * 
	 * @param metadataId
	 * @return
	 */
	public MMMetaModel queryMetaModelBySql(String metadataId) {

		try {
			MMMetaModel metaModel = metadataDao.queryMetaModelBySql(metadataId);
			metaModelDao.genStorePositionByApi(metaModel);

			return metaModel;
		}
		catch (MetadataNotFoundException e) {

			log.warn(e.getMessage());
			if (log.isDebugEnabled()) {
				log.warn("", e);
			}
			return null;
		}
		catch (Exception e) {
			log.warn(e.getMessage());
			return null;
		}

	}

	public boolean existMetadata(String parentId, MMMetadata rootMetadata) {
		return metadataDao.existMetadata(parentId, rootMetadata);
	}

	public List<AbstractMetadata> genChildMetadatas(List<AbstractMetadata> parentMetadatas, MMMetaModel metaModel) {
		// 父结点的个数
		int size = parentMetadatas.size();
		// 需查询库表的次数
		int degree = size / Constants.MAX_EXPRESSION_SIZE + 1;
		List<AbstractMetadata> childMetadatas = new ArrayList<AbstractMetadata>(size);
		int fromIndex, toIndex;
		for (int i = 0; i < degree; i++) {
			fromIndex = i * Constants.MAX_EXPRESSION_SIZE;
			toIndex = (i == degree - 1) ? size : fromIndex + Constants.MAX_EXPRESSION_SIZE;
			childMetadatas.addAll(metadataDao.queryChildMetadatas(parentMetadatas.subList(fromIndex, toIndex),
					metaModel));
		}

		return childMetadatas;
	}
	
	public List<MMMetadata> genChildrenMetadatasId(String aNamespace){
		return metadataDao.genChildrenMetadatasId(aNamespace);
	}

	public List<MMDDependency> genDbDependencies(String aNamespace) {
		return dependencyDao.genDependencies(aNamespace);
	}

	public void setDependencyDao(IDependencyDao dependencyDao) {
		this.dependencyDao = dependencyDao;
	}

	public List<MMDDependency> genDbDependencies(Set<String> metadataNamespaces) {
		List<MMMetadata> metadatas = new ArrayList<MMMetadata>();
		for(String namespace:metadataNamespaces){
			// 包含自身
			metadatas.addAll(genChildrenMetadatasId(namespace));
		}
		
		List<MMDDependency> dependencies = new ArrayList<MMDDependency>();
		// 元数据结点的个数
		int size = metadatas.size();
		// 需查询库表的次数
		int degree = size / Constants.MAX_EXPRESSION_SIZE + 1;
		int fromIndex, toIndex;
		for (int i = 0; i < degree; i++) {
			fromIndex = i * Constants.MAX_EXPRESSION_SIZE;
			toIndex = (i == degree - 1) ? size : fromIndex + Constants.MAX_EXPRESSION_SIZE;
			dependencies.addAll(dependencyDao.genDependencies(metadatas.subList(fromIndex, toIndex)));
		}

		return dependencies;
	}

	/* (non-Javadoc)  
	  * @param id
	  * @return  
	  * @see com.pactera.edg.am.metamanager.extractor.increment.IGenMetadataService#getParId(java.lang.String)  
	  */  
	public String getParId(String id) {
		// TODO Auto-generated method stub
		return metadataDao.getParId(id);
	}
}
