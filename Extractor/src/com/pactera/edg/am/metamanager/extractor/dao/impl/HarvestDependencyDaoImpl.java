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

package com.pactera.edg.am.metamanager.extractor.dao.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.AuditDependencyMapper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.CreateHarvestDependencyHelper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.DeleteHarvestDependencyHelper;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

/**
 * 待审核元数据依赖关系操作DAO实现类
 * 
 * @author user
 * @version 1.0 Date: Oct 5, 2009
 * 
 */
public class HarvestDependencyDaoImpl extends DaoBaseServiceImpl implements IHarvestDependencyDao {

	private Log log = LogFactory.getLog(HarvestDependencyDaoImpl.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestDependencyDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadCreate(AppendMetadata createAMetadata) {
		try {
			List<MMDDependency> dependenciesList = createAMetadata.getDDependencies();
			if (dependenciesList != null && !dependenciesList.isEmpty()) {
				// 数据不为空
				String sql = super.getSql("LOAD_CREATE_HARVEST_DEPENDENCY");
				batchLoadCreate(sql, dependenciesList);
			}
		}
		finally {
			super.clear();
		}

	}

	private void batchLoadCreate(String sql, final List<MMDDependency> dependenciesList) {
		helper = new CreateHarvestDependencyHelper(super.getBatchSize());
		((CreateHarvestDependencyHelper) helper).setDependenciesList(dependenciesList);
		super.getJdbcTemplate().execute(sql, helper);
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestDependencyDao#batchLoadDelete(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadDelete(AppendMetadata deleteAMetadata) {
		try {
			String sql = super.getSql("LOAD_DELETE_HARVEST_DEPENDENCY");
			List<MMDDependency> dependencies = deleteAMetadata.getDDependencies();
			if (dependencies != null) {
				// 有需要刪除的依賴關係
				helper = new DeleteHarvestDependencyHelper(super.getBatchSize());
				((DeleteHarvestDependencyHelper) helper).setDeleteDependencies(dependencies);

				super.getJdbcTemplate().execute(sql, helper);
			}
			else {
				String logMsg = "添加待删除依赖关系到审核表记录数:0";
				log.info(logMsg);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
			}
		}
		finally {
			super.clear();
		}
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestDependencyDao#genAuditDependencies()
	 */
	public List<AuditDependency> genAuditDependencies() {
		// 获取待审核表中的元数据
		String sql = GenSqlUtil.getSql("QUERY_AUDIT_DEPENDENCY");
		return super.getJdbcTemplate().query(sql,
				new Object[] { AdapterExtractorContext.getInstance().getTaskInstanceId() }, new AuditDependencyMapper());
		
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestDependencyDao#updateOldDatas()
	 */
	public void updateOldDatas() {
		String sql = GenSqlUtil.getSql("UPDATE_OLD_AUDIT_DEPENDENCY");

		int updateSize = super.getJdbcTemplate().update(
				sql,
				new Object[] { AdapterExtractorContext.getInstance().getTaskInstanceId(),
						AdapterExtractorContext.getInstance().getTaskInstanceId() });
		
		log.info("更新旧有待审核依赖关系的状态数:" + updateSize);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "更新旧有待审核依赖关系的状态数:" + updateSize);
		
	}

}
