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

package com.pactera.edg.am.metamanager.extractor.audit.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.pactera.edg.am.metamanager.extractor.audit.IAuditMetadataService;
import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.control.IExtractorConfLoader;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetaModelDao;
import com.pactera.edg.am.metamanager.extractor.ex.MetaModelNotFoundException;
import com.pactera.edg.am.metamanager.extractor.ex.SpringContextLoadException;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;

/**
 * 审核服务操作类,旨在对已确认的审核数据进行分类,得到需要添加,修改,删除的元数据,依赖关系
 * 
 * @author user
 * @version 1.0 Date: Oct 11, 2009
 * 
 */
public class AuditMetadataServiceImpl implements IAuditMetadataService {

	/**
	 * 审核元数据DAO接口
	 */
	private IHarvestMetadataDao harvestMetadataDao;

	/**
	 * 元模型DAO接口
	 */
	private IMetaModelDao metaModelDao;

	/**
	 * 审核依赖关系DAO接口
	 */
	private IHarvestDependencyDao harvestDependencyDao;

	public void setHarvestDependencyDao(IHarvestDependencyDao harvestDependencyDao) {
		this.harvestDependencyDao = harvestDependencyDao;
	}

	public void setMetaModelDao(IMetaModelDao metaModelDao) {
		this.metaModelDao = metaModelDao;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.audit.IAuditMetadataService#genAuditAMetadata()
	 */
	public Map<Operation, AppendMetadata> genAuditAMetadata() throws MetaModelNotFoundException {
		initProperties();

		Map<Operation, AppendMetadata> aMetadata = convertToAppendMetadatas(harvestMetadataDao.genAuditAMetadata());

		List<AuditDependency> auditDependencies = harvestDependencyDao.genAuditDependencies();
		convertToAppendDependencies(aMetadata, auditDependencies);

		return aMetadata;
	}

	private void initProperties() {

		try {
			IExtractorConfLoader confLoader = (IExtractorConfLoader) ExtractorContextLoader
					.getBean(IExtractorConfLoader.SPRING_NAME);
			confLoader.queryUser(AdapterExtractorContext.getInstance().getTaskInstanceId());

		}
		catch (SpringContextLoadException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 将从待审核依赖关系表中获取的依赖关系列表,转换成符合入库操作用的添加,删除的依赖关系对象列表
	 * 
	 * @param metadata
	 * @param auditDependencies
	 */
	private void convertToAppendDependencies(Map<Operation, AppendMetadata> metadata,
			List<AuditDependency> auditDependencies) {
		List<MMDDependency> createDependencies = new ArrayList<MMDDependency>();
		List<MMDDependency> modifyDependencies = new ArrayList<MMDDependency>();
		List<MMDDependency> deleteDependencies = new ArrayList<MMDDependency>();

		for (AuditDependency auditDependency : auditDependencies) {
			String type = auditDependency.getType();
			if ("1".equals(type)) {
				createDependencies.add(auditDependency);
			}
			else if ("2".equals(type)) {
				modifyDependencies.add(auditDependency);
			}
			else {
				deleteDependencies.add(auditDependency);
			}
		}

		metadata.get(Operation.CREATE).setDDependencies(createDependencies);
		metadata.get(Operation.MODIFY).setDDependencies(modifyDependencies);
		metadata.get(Operation.DELETE).setDDependencies(deleteDependencies);

	}

	/**
	 * 将从待审核元数据表中获取的元数据列表,转换成符合入库操作用的添加,修改,删除的元数据列表
	 * 
	 * @param auditMetadatas
	 * @return
	 * @throws MetaModelNotFoundException
	 */
	private Map<Operation, AppendMetadata> convertToAppendMetadatas(List<AuditMetadata> auditMetadatas)
			throws MetaModelNotFoundException {

		// 对数据进行排序
		Collections.sort(auditMetadatas, new Comparator<AuditMetadata>() {

			public int compare(AuditMetadata o1, AuditMetadata o2) {
				int compareInt = o1.getClassifierId().compareTo(o2.getClassifierId());
				if (compareInt == 0) {
					compareInt = o1.getOperType().compareTo(o2.getOperType());
					if (compareInt == 0) {
						compareInt = o1.getId().compareTo(o2.getId());
					}
				}
				return compareInt;
			}
		});

		Map<String, MMMetaModel> metaModelCaches = genMetaModelCaches(auditMetadatas);

		Set<MMMetaModel> createMetaModels = new HashSet<MMMetaModel>();
		Set<MMMetaModel> modifyMetaModels = new HashSet<MMMetaModel>();
		Set<MMMetaModel> deleteMetaModels = new HashSet<MMMetaModel>();
		// 将需要添加的,修改的,删除的元模型及数据,分别放入相应的容器中
		// 类型1:添加
		genMetadatas(createMetaModels, "1", auditMetadatas, metaModelCaches);
		// 类型2:删除
		genMetadatas(deleteMetaModels, "2", auditMetadatas, metaModelCaches);
		// 类型3:修改
		genMetadatas(modifyMetaModels, "3", auditMetadatas, metaModelCaches);

		AppendMetadata aMetadata = new AppendMetadata();
		MMMetadata m = new MMMetadata();
		aMetadata.setMetadata(m);
		aMetadata.setMetaModel(new MMMetaModel());
		AppendMetadata createAMetadata = aMetadata.cloneAppendMetadata();
		AppendMetadata modifyAMetadata = aMetadata.cloneAppendMetadata();
		AppendMetadata deleteAMetadata = aMetadata.cloneAppendMetadata();

		createAMetadata.setChildMetaModels(createMetaModels);
		modifyAMetadata.setChildMetaModels(modifyMetaModels);
		deleteAMetadata.setChildMetaModels(deleteMetaModels);

		Map<Operation, AppendMetadata> aMetadatas = new HashMap<Operation, AppendMetadata>(3);
		aMetadatas.put(Operation.CREATE, createAMetadata);
		aMetadatas.put(Operation.MODIFY, modifyAMetadata);
		aMetadatas.put(Operation.DELETE, deleteAMetadata);

		return aMetadatas;
	}

	private Map<String, MMMetaModel> genMetaModelCaches(List<AuditMetadata> auditMetadatas)
			throws MetaModelNotFoundException {
		Map<String, MMMetaModel> metaModelCaches = new HashMap<String, MMMetaModel>();

		for (AuditMetadata auditMetadata : auditMetadatas) {
			String metaModelCode = auditMetadata.getClassifierId();
			if (!metaModelCaches.containsKey(metaModelCode)) {
				// 还没出现过
				MMMetaModel metaModel = new MMMetaModel();
				metaModel.setCode(metaModelCode);
				metaModelCaches.put(metaModelCode, metaModel);
			}
		}

		for (Iterator<MMMetaModel> iter = metaModelCaches.values().iterator(); iter.hasNext();) {
			metaModelDao.genStorePositionByApi(iter.next());
		}
		return metaModelCaches;
	}

	private void genMetadatas(Set<MMMetaModel> metaModels, String type, List<AuditMetadata> auditMetadatas,
			Map<String, MMMetaModel> metaModelCaches) {

		MMMetaModel metaModel = null;
		String metaModelCode = null;
		for (AuditMetadata auditMetadata : auditMetadatas) {
			String cntType = auditMetadata.getOperType();
			if (!type.equals(cntType))
				// 只处理当前类型的数据
				continue;

			String cntMetaModelCode = auditMetadata.getClassifierId();
			if (metaModelCode == null) {
				metaModelCode = cntMetaModelCode;
				metaModel = metaModelCaches.get(cntMetaModelCode).cloneOnlyMetaModel();
			}

			if (!cntMetaModelCode.equals(metaModelCode)) {
				metaModel.setHasMetadata(true);
				metaModels.add(metaModel);
				metaModelCode = cntMetaModelCode;
				metaModel = metaModelCaches.get(cntMetaModelCode).cloneOnlyMetaModel();
			}

			metaModel.addMetadata(auditMetadata);

		}
		if (metaModel != null) {
			metaModel.setHasMetadata(true);
			metaModels.add(metaModel);
		}

	}

	public void setHarvestMetadataDao(IHarvestMetadataDao harvestMetadataDao) {
		this.harvestMetadataDao = harvestMetadataDao;
	}

}
