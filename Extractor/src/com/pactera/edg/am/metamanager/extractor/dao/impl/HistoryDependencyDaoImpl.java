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
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.DeleteDependencyHelper;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

public class HistoryDependencyDaoImpl extends DaoBaseServiceImpl implements IHistoryDependencyDao {

	private Log log = LogFactory.getLog(HistoryDependencyDaoImpl.class);

	public void batchLoadCreate(AppendMetadata aMetadata) {
		List<MMDDependency> dependenciesList = aMetadata.getDDependencies();
		if (dependenciesList != null && !dependenciesList.isEmpty()) {
			batchLoadCreate(dependenciesList);
		}
	}

	private void batchLoadCreate(List<MMDDependency> dependenciesList) {
		helper = new DeleteDependencyHelper(super.getBatchSize());
		((DeleteDependencyHelper) helper).setDependenciesList(dependenciesList);
		long cntTime = AdapterExtractorContext.getInstance().getGlobalTime();
		String createHistorySql = super.getSql("LOAD_HISTORY_CREATE_DEPENDENCY");
		createHistorySql = createHistorySql.replaceAll(Constants.SQL_END_TIME, String.valueOf(cntTime));
		try {
			super.getJdbcTemplate().execute(createHistorySql, helper);
		}
		catch (Exception e) {
			// 写依赖关系至历史依赖关系表时发生异常，跳过，不影响下一步的操作，但要求数据回滚
			log.error("待删除的元数据依赖关系添加至历史表时，发生异常，将其捕获，不影响下一步的进行，但要求将部分已写至历史表的信息回滚", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"待删除的元数据依赖关系添加至历史表时，发生异常，将其捕获，不影响下一步的进行，但要求将部分已写至历史表的信息回滚");
			rollback();
		}
	}

	/**
	 * 回滚本次操作中,已经写在历史元数据依赖关系表的数据
	 */
	public void rollback() {
		String dependencySql = GenSqlUtil.getSql("ROLLBACK_HISTORY_DEPENDENCY");
		// dependencySql = dependencySql.replaceAll(Constants.SQL_END_TIME,
		// Long.toString(AdapterExtractorContext
		// .getInstance().getGlobalTime()));

		super.getJdbcTemplate().update(dependencySql,
				new Object[] { Long.toString(AdapterExtractorContext.getInstance().getGlobalTime()) });
	}
}
