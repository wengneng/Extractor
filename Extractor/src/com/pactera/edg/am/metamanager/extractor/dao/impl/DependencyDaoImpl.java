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

import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.CreateDependencyHelper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.DbDependencyMapper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.DeleteDependencyHelper;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 元数据依赖关系,操作数据库层面的DAO实现
 * 
 * @author user
 * @version 1.0 Date: Jul 29, 2009
 * 
 */
public class DependencyDaoImpl extends DaoBaseServiceImpl implements IDependencyDao {

	private Log log = LogFactory.getLog(DependencyDaoImpl.class);

	private void batchLoadCreate(String sql, final List<MMDDependency> dependenciesList) {
		helper = new CreateDependencyHelper(super.getBatchSize());
		((CreateDependencyHelper) helper).setDependenciesList(dependenciesList);
		super.getJdbcTemplate().execute(sql, helper);
	}

	private void batchLoadDelete(List<MMDDependency> dependenciesList) {
		helper = new DeleteDependencyHelper(super.getBatchSize());
		((DeleteDependencyHelper) helper).setDependenciesList(dependenciesList);
		// 再删除依赖关系记录
		String deleteSql = super.getSql("LOAD_DELETE_DEPENDENCY");
		super.getJdbcTemplate().execute(deleteSql, helper);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IDependencyDao#genDependencies(java.lang.String)
	 */
	public List<MMDDependency> genDependencies(final String aNamespace) {
		String querySql = super.getSql("QUERY_DEPENDENCY");
		querySql = querySql.replaceAll(Constants.SQL_NAMESPACE, aNamespace);

		return (List<MMDDependency>) super.getJdbcTemplate().query(querySql, new DbDependencyMapper());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IDependencyDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadCreate(AppendMetadata createAMetadata) {
		List<MMDDependency> dependenciesList = null;
		dependenciesList = createAMetadata.getDDependencies();
		try {
			if (dependenciesList != null && !dependenciesList.isEmpty()) {
				String sql = super.getSql("LOAD_CREATE_DEPENDENCY");
				batchLoadCreate(sql, dependenciesList);
			}
			else {
				String logMsg = "添加依赖关系记录数:0";
				log.info(logMsg);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
			}
		}
		finally {
			super.clear();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IDependencyDao#batchLoadDelete(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadDelete(AppendMetadata deleteAMetadata) {
		try {
			List<MMDDependency> dependenciesList = deleteAMetadata.getDDependencies();
			if (dependenciesList != null && !dependenciesList.isEmpty()) {
				// 数据不为空
				batchLoadDelete(dependenciesList);
			}
			else {
				String logMsg = "删除依赖关系记录数:0";
				log.info(logMsg);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
			}
		}
		finally {
			super.clear();
		}
	}

	public Collection<? extends MMDDependency> genDependencies(List<MMMetadata> metadatas) {
		String querySql = super.getSql("QUERY_DEPENDENCY2");
		StringBuilder sb = new StringBuilder();
		for (MMMetadata metadata : metadatas) {
			sb.append(", '").append(metadata.getId()).append("'");
		}
		querySql = querySql.replaceAll(Constants.SQL_INSTANCE_ID, sb.toString());

		return (List<MMDDependency>) super.getJdbcTemplate().query(querySql, new DbDependencyMapper());
	}

}
