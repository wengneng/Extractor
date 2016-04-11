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


package com.pactera.edg.am.metamanager.extractor.dao.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.dao.IAuditLoadDependencyDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

/**
 * 审核入库元数据依赖关系操作DAO实现类
 *
 * @author user
 * @version 1.0  Date: Oct 13, 2009
 *
 */
public class AuditLoadDependencyDaoImpl extends JdbcDaoSupport implements IAuditLoadDependencyDao {

	private Log log = LogFactory.getLog(AuditLoadDependencyDaoImpl.class);
	
	protected String getTaskInstanceIdSQL() {
		String taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();
		return "'" + taskInstanceId + "'";
	}
	
	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IAuditDependencyDao#batchLoadCreate()
	 */
	public void auditLoadCreate() {
		String sql = GenSqlUtil.getSql("LOAD_AUDIT_TO_CREATE_DEPENDENCY");
		sql = sql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());

		super.getJdbcTemplate().execute(sql);

		log.info("添加已确认的需添加待审核元数据依赖关系完成!");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "添加已确认的需添加待审核元数据依赖关系完成!");
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IAuditDependencyDao#batchLoadDelete()
	 */
	public void auditLoadDelete() {
		String sql = GenSqlUtil.getSql("LOAD_AUDIT_TO_DELETE_DEPENDENCY");
		sql = sql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());

		super.getJdbcTemplate().execute(sql);

		log.info("删除已确认的需删除待审核元数据依赖关系完成!");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "删除已确认的需删除待审核元数据依赖关系完成!");
		
		batchLoadHistoryDelete();
	}
	
	/**
	 * 将需删除的元数据依赖关系写入历史表
	 */
	private void batchLoadHistoryDelete(){
		long cntTime = AdapterExtractorContext.getInstance().getGlobalTime();
		String sql = GenSqlUtil.getSql("LOAD_AUDIT_TO_HISTORY_DELETE_DEPENDENCY");
		sql = sql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());
		sql = sql.replaceAll(Constants.SQL_END_TIME, String.valueOf(cntTime));

		super.getJdbcTemplate().execute(sql);

		log.info("将已删除的元数据依赖关系写入至历史表完成!");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "将已删除的元数据依赖关系写入至历史表完成!");
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IAuditLoadDependencyDao#delete()
	 */
	public void delete() {
		String sql = GenSqlUtil.getSql("DELETE_AUDIT_DEPENDENCY");
		sql = sql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());

		super.getJdbcTemplate().execute(sql);
	}

}
