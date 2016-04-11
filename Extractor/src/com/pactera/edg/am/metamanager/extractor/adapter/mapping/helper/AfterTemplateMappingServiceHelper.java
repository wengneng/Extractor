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

package com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * @since 添加产品主映映射的处理
 * @since 添加对信息分类与信息项关系的处理,只允许最后一级分类与信息项有依赖
 * 
 * @author user
 * @version 1.0 Date: Mar 23, 2011
 * 
 */
public class AfterTemplateMappingServiceHelper {

	private AppendMetadata aMetadata;

	private boolean hasProduct = false;

	public AfterTemplateMappingServiceHelper(AppendMetadata aMetadata)
	{
		this.aMetadata = aMetadata;
	}

	public void afterExtracted() {
		iterMetaModel(aMetadata.getChildMetaModels());

		if (hasProduct) {
			// 删除产品相关的依赖关系
			deleteProductDependencids(aMetadata.getDDependencies());
			// 删除了产品相关的依赖关系之后,还得到元模型中,找到产品映射,产品主映映射删除相关的产品映射,产品主映映射信息...悲剧中的悲剧
			deleteProductMapping(aMetadata.getChildMetaModels());
		}
	}

	private void deleteProductMapping(Set<MMMetaModel> metaModels) {
		for (MMMetaModel metaModel : metaModels) {
			String metaModelCode = metaModel.getCode(); // 元模型
			if (metaModelCode.equals("AmProductMapping") || metaModelCode.equals("AmProductPKMapping")) {
				deleteMetadatas(metaModel.getMetadatas());
				break;
			}
			deleteProductMapping(metaModel.getChildMetaModels());
		}
	}

	private void deleteProductDependencids(List<MMDDependency> dependencies) {
		deleteProductDependencids(dependencies.iterator(), "AmProductMapping");
//		deleteProductDependencids(dependencies.iterator(), "AmProductPKMapping");

		// 需再查找并删除一遍,原因在于产品主映映射与字段间的依赖,可能先于产品映射与产品主映映射间的依赖,悲剧...
		for (Iterator<MMDDependency> dependencyIter = dependencies.iterator(); dependencyIter.hasNext();) {
			MMDDependency dependency = dependencyIter.next();
			if (dependency.getOwnerMetadata().getCode() == null) { // 产品主映映射与字段间有依赖时,产品主映映射只可能出现在Owner
				dependencyIter.remove();
			}
		}
	}

	private void deleteProductDependencids(Iterator<MMDDependency> dependencyIter, String classifierId) {
		for (; dependencyIter.hasNext();) {
			MMDDependency dependency = dependencyIter.next();
			if (dependency.getOwnerMetadata().getCode() == null) { // 产品只可能出现在Owner
				// 发现这个还不能随便设,需判断其类型
				// dependency.getValueMetadata().setCode(null); //
				// 产品映射,1.需删除产品映射的前后依赖关系;2.需删除产品映射的结点
//				MMMetadata valueMetadata = dependency.getValueMetadata();
//				if (valueMetadata.getClassifierId().equals(classifierId)) {
//					// 是产品映射的元数据
//					valueMetadata.setCode(null);
//				}
				dependencyIter.remove();
			}
		}
	}

	private void iterMetaModel(Set<MMMetaModel> metaModels) {
		for (MMMetaModel metaModel : metaModels) {
			String metaModelCode = metaModel.getCode(); // 元模型
			// ----------处理产品----------------------
			if (metaModelCode.indexOf("AmProductClass") > -1) { // 第四层产品分类
				if (metaModel.isHasMetadata()) {
					// 有元数据
					deleteChildProductMetadatas(metaModel.getMetadatas());
				}
			}
			else if (metaModelCode.equals("AmProduct")) {
				// 属于产品的元模型,需要在元模型引用的产品数据中,删除需删除的数据
				hasProduct = true;
				deleteMetadatas(metaModel.getMetadatas());
			}
			// --------处理产品结束----------------------
			// --------处理分类项-----------------------
			else if (metaModelCode.indexOf("CdbClass") > -1) {// 第三层分类
				if (metaModel.isHasMetadata()) {
					// 有元数据
					deleteChildClassItemMetadatas(metaModel.getMetadatas());
				}
			}
			else if (metaModelCode.equals("CdbClassItem")) {
				// 属于分类项的元模型,需要在元模型引用的分类项数据中,删除需删除的数据
				deleteMetadatas(metaModel.getMetadatas());
			}

			// --------处理分类项结束--------------------
			// --------处理信息项-----------------------
			else if (metaModelCode.startsWith("FinanceInfoClassification")) {
				// 元模型是信息大类,小类,三级分类,四级分类中的其中之一
				deleteInfoTypeSuperfluousDependencies(metaModel.getMetadatas());
			}
			// --------处理信息项结束-------------------

			Set<MMMetaModel> childrenMetaModel = metaModel.getChildMetaModels();
			if (childrenMetaModel.size() > 0) {
				iterMetaModel(childrenMetaModel);
			}
		}
	}

	private void deleteChildClassItemMetadatas(List<AbstractMetadata> metadatas) {
		for (AbstractMetadata metadata : metadatas) { // 分类数据
			List<AbstractMetadata> childrenMetadatas = metadata.getChildrenMetadatas();
			for (AbstractMetadata childMetadata : childrenMetadatas) { // 判断子元数据是否有属于分类项的数据
				if (childMetadata.getClassifierId().indexOf("CdbClass") > -1
						&& !childMetadata.getClassifierId().equals("CdbClassItem")) {
					deleteChildClassItemMetadata(childrenMetadatas);

					break;
				}
			}

		}
	}

	private void deleteChildClassItemMetadata(List<AbstractMetadata> metadatas) {
		for (Iterator<AbstractMetadata> metadataIter = metadatas.iterator(); metadataIter.hasNext();) {
			AbstractMetadata metadata = metadataIter.next();
			if (metadata.getClassifierId().equals("CdbClassItem")) { // 是分类项,则删除之
				((MMMetadata) metadata).setCode(null); // 将原始数据的CODE设为NULL,后续以此为依据删除列表中的数据
				metadataIter.remove(); // 从childrenMetadatas列表中去除了该结点,但并不代表该结点的原始数据已经删除
			}
		}
	}

	private void deleteInfoTypeSuperfluousDependencies(List<AbstractMetadata> metadatas) {
		for (AbstractMetadata metadata : metadatas) {
			// 元数据没有叶子结点,且有依赖关系，则将其祖父结点的依赖关系删除
			if (metadata.getChildrenMetadatas().size() == 0 && existDependencies(metadata)) {
				deleteInfoTypeParentDependencies(metadata.getParentMetadata());
			}
		}

	}

	private void deleteInfoTypeParentDependencies(MMMetadata parentMetadata) {

		if ("CdbFinanceSubject".equals(parentMetadata.getClassifierId()))
			// 到主题了，则到此为止
			return;

		if (existDependencies(parentMetadata)) {
			// 当前结点存在依赖关系,则删除依赖关系
			deleteDependencies(parentMetadata);
			// 删除完当前结点的依赖关系，再删除父结点的依赖关系
			deleteInfoTypeParentDependencies(parentMetadata.getParentMetadata());
		}
	}

	private void deleteDependencies(MMMetadata metadata) {
		List<MMDDependency> dependencies = aMetadata.getDDependencies();

		for (Iterator<MMDDependency> dependencyIter = dependencies.iterator(); dependencyIter.hasNext();) {
			MMDDependency dependency = dependencyIter.next();
			if (dependency.getOwnerMetadata().equals(metadata))
				dependencyIter.remove();
		}

	}

	private boolean existDependencies(AbstractMetadata metadata) {
		MMMetadata mMetadata = (MMMetadata) metadata;
		List<MMDDependency> dependencies = aMetadata.getDDependencies();
		for (MMDDependency dependency : dependencies) {
			if (dependency.getOwnerMetadata().equals(mMetadata)) // 如果找到一条,则认为其有依赖关系
				return true;
		}
		return false;
	}

	private void deleteMetadatas(List<AbstractMetadata> metadatas) {
		for (Iterator<AbstractMetadata> metadataIter = metadatas.iterator(); metadataIter.hasNext();) {
			AbstractMetadata metadata = metadataIter.next();
			if (metadata.getCode() == null) { // 之前预设的一个值
				metadataIter.remove();

			}
		}
	}

	private void deleteChildProductMetadatas(List<AbstractMetadata> metadatas) {
		for (AbstractMetadata metadata : metadatas) {
			List<AbstractMetadata> childrenMetadatas = metadata.getChildrenMetadatas();
			for (AbstractMetadata childMetadata : childrenMetadatas) { // 判断子元数据是否有属于产品分类的数据
				if (childMetadata.getClassifierId().indexOf("AmProductClass") > -1) {
					deleteChildProductMetadata(childrenMetadatas);

					// deleteChildProductMetadatas(childMetadata.getChildrenMetadatas());

					break;
				}
			}

		}

	}

	private void deleteChildProductMetadata(List<AbstractMetadata> metadatas) {
		for (Iterator<AbstractMetadata> metadataIter = metadatas.iterator(); metadataIter.hasNext();) {
			AbstractMetadata metadata = metadataIter.next();
			if (metadata.getClassifierId().equals("AmProduct")) { // 是产品,则删除之
				((MMMetadata) metadata).setCode(null); // 将原始数据的CODE设为NULL,后续以此为依据删除列表中的数据
				metadataIter.remove(); // 从childrenMetadatas列表中去除了该结点,但并不代表该结点的原始数据已经删除
			}
		}
	}

}
