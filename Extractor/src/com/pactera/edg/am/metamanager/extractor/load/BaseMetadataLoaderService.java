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

package com.pactera.edg.am.metamanager.extractor.load;

import com.pactera.edg.am.metamanager.extractor.dao.IAuditLoadCompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.IAuditLoadDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.IAuditLoadMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.ICompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.IDeconstructorDao;
import com.pactera.edg.am.metamanager.extractor.dao.IDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestCompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryCompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataAlterDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao;

public class BaseMetadataLoaderService {

	/**
	 * 审核入库元数据依赖关系操作DAO
	 */
	private IAuditLoadDependencyDao auditLoadDependencyDao;

	/**
	 * 审核入库元数据组合关系操作DAO
	 */
	private IAuditLoadCompositionDao auditLoadCompositionDao;

	/**
	 * 审核入库元数据操作DAO
	 */
	private IAuditLoadMetadataDao auditLoadMetadataDao;

	/**
	 * 元数据变更DAO记录元数据表的添加,修改,删除的数据
	 */
	private IMetadataAlterDao metadataAlterDao;

	/**
	 * 元数据DAO,用于操作元数据表,包括查询,修改,添加,删除元数据操作
	 */
	private IMetadataDao metadataDao;

	/**
	 * 组合关系DAO接口,用于操作元数据间的组合关系,包括添查询,修改,添加中,删除组合关系操作
	 */
	private ICompositionDao compositionDao;

	/**
	 * 依赖关系DAO接口,用于操作元数据间的依赖关系,包括添查询,修改,添加中,删除依赖关系操作
	 */
	private IDependencyDao dependencyDao;

	/**
	 * 元数据DAO,用于操作元数据表,包括查询,修改,添加,删除元数据操作
	 */
	private IHarvestMetadataDao harvestMetadataDao;

	/**
	 * 组合关系DAO接口,用于操作元数据间的组合关系,包括添查询,修改,添加中,删除组合关系操作
	 */
	private IHarvestCompositionDao harvestCompositionDao;

	/**
	 * 依赖关系DAO接口,用于操作元数据间的依赖关系,包括添查询,修改,添加中,删除依赖关系操作
	 */
	private IHarvestDependencyDao harvestDependencyDao;
	

	/**
	 * 历史元数据DAO接口
	 */
	private IHistoryMetadataDao historyMetadataDao;

	private IHistoryDependencyDao historyDependencyDao;
	
	private IHistoryCompositionDao historyCompositionDao;

	/**
	 * 析构DAO接口
	 */
	private IDeconstructorDao deconstructorDao;

	public IHistoryCompositionDao getHistoryCompositionDao() {
		return historyCompositionDao;
	}

	public void setHistoryCompositionDao(IHistoryCompositionDao historyCompositionDao) {
		this.historyCompositionDao = historyCompositionDao;
	}

	public IHistoryDependencyDao getHistoryDependencyDao() {
		return historyDependencyDao;
	}

	public void setHistoryDependencyDao(IHistoryDependencyDao historyDependencyDao) {
		this.historyDependencyDao = historyDependencyDao;
	}

	public void setMetadataDao(IMetadataDao metadataDao) {
		this.metadataDao = metadataDao;
	}

	public void setCompositionDao(ICompositionDao compositionDao) {
		this.compositionDao = compositionDao;
	}

	public void setDependencyDao(IDependencyDao dependencyDao) {
		this.dependencyDao = dependencyDao;
	}

	public void setHistoryMetadataDao(IHistoryMetadataDao historyMetadataDao) {
		this.historyMetadataDao = historyMetadataDao;
	}

	public void setHarvestMetadataDao(IHarvestMetadataDao harvestMetadataDao) {
		this.harvestMetadataDao = harvestMetadataDao;
	}

	public void setHarvestCompositionDao(IHarvestCompositionDao harvestCompositionDao) {
		this.harvestCompositionDao = harvestCompositionDao;
	}

	public void setHarvestDependencyDao(IHarvestDependencyDao harvestDependencyDao) {
		this.harvestDependencyDao = harvestDependencyDao;
	}

	public void setMetadataAlterDao(IMetadataAlterDao metadataAlterDao) {
		this.metadataAlterDao = metadataAlterDao;
	}

	public void setAuditLoadDependencyDao(IAuditLoadDependencyDao auditLoadDependencyDao) {
		this.auditLoadDependencyDao = auditLoadDependencyDao;
	}

	public void setAuditLoadCompositionDao(IAuditLoadCompositionDao auditLoadCompositionDao) {
		this.auditLoadCompositionDao = auditLoadCompositionDao;
	}

	public void setAuditLoadMetadataDao(IAuditLoadMetadataDao auditLoadMetadataDao) {
		this.auditLoadMetadataDao = auditLoadMetadataDao;
	}

	public IAuditLoadDependencyDao getAuditLoadDependencyDao() {
		return auditLoadDependencyDao;
	}

	public IAuditLoadCompositionDao getAuditLoadCompositionDao() {
		return auditLoadCompositionDao;
	}

	public IAuditLoadMetadataDao getAuditLoadMetadataDao() {
		return auditLoadMetadataDao;
	}

	public IMetadataAlterDao getMetadataAlterDao() {
		return metadataAlterDao;
	}

	public IMetadataDao getMetadataDao() {
		return metadataDao;
	}

	public ICompositionDao getCompositionDao() {
		return compositionDao;
	}

	public IDependencyDao getDependencyDao() {
		return dependencyDao;
	}

	public IHarvestMetadataDao getHarvestMetadataDao() {
		return harvestMetadataDao;
	}

	public IHarvestCompositionDao getHarvestCompositionDao() {
		return harvestCompositionDao;
	}

	public IHarvestDependencyDao getHarvestDependencyDao() {
		return harvestDependencyDao;
	}

	public IHistoryMetadataDao getHistoryMetadataDao() {
		return historyMetadataDao;
	}

	public IDeconstructorDao getDeconstructorDao() {
		return deconstructorDao;
	}

	public void setDeconstructorDao(IDeconstructorDao deconstructorDao) {
		this.deconstructorDao = deconstructorDao;
	}
}
