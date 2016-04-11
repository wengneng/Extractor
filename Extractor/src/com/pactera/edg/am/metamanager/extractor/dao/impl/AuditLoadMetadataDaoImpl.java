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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.core.util.DateUtil;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.IAuditLoadMetadataDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 审核入库元数据操作DAO实现类
 * 
 * @author user
 * @version 1.0 Date: Oct 12, 2009
 * 
 */
public class AuditLoadMetadataDaoImpl extends HistoryMetadataDaoImpl implements IAuditLoadMetadataDao {

	private Log log = LogFactory.getLog(AuditLoadMetadataDaoImpl.class);

	protected String getTaskInstanceIdSQL() {
		String taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();
		return "'" + taskInstanceId + "'";
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IAuditMetadataDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void auditLoadCreate(AppendMetadata createAMetadata) {
		sql = super.getSql("LOAD_AUDIT_TO_CREATE_METADATA");
		count = 0;
		String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);

		Set<MMMetaModel> metaModels = createAMetadata.getChildMetaModels();
		for (MMMetaModel metaModel : metaModels) {
			batchLoadCreate(metaModel);
		}
		String logMsg = new StringBuilder("添加已确认的待审核元数据至元数据表记录数:").append(count).append(",开始时间:").append(startTime)
				.append(",结束时间:").append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT))
				.toString();
		log.info(logMsg);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IAuditMetadataDao#batchLoadModify(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void auditLoadModify(AppendMetadata modifyAMetadata) {
		String deleteMdSQL = super.getSql("DELETE_METADATA_BEFORE_MODIFY_AUDIT_TO_METADATA");
		deleteMdSQL = deleteMdSQL.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());
		super.getJdbcTemplate().execute(deleteMdSQL);
		
		String sql = super.getSql("LOAD_AUDIT_TO_MODIFY_METADATA"); //FIXME 该SQL被重复执行多次
		Set<MMMetaModel> metaModels = modifyAMetadata.getChildMetaModels();
		for (MMMetaModel metaModel : metaModels) {
			String metaModelSql = setAttrInSql(sql, metaModel);
			super.getJdbcTemplate().execute(metaModelSql);
		}
	}

	private String setAttrInSql(String sql, MMMetaModel metaModel) {
		Map<String, String> mAttrs = metaModel.getMAttrs();
		StringBuilder mAttrsString = new StringBuilder();
		for (Iterator<String> iter = mAttrs.values().iterator(); iter.hasNext();) {
			String position = iter.next();
			mAttrsString.append(",").append(position);
		}
		// 替换元数据表中的属性
		String tmpSql = sql.replaceAll(Constants.SQL_ATTR_KEYS_FLAG, mAttrsString.toString());
		tmpSql = tmpSql.replaceAll(Constants.SQL_START_TIME, String.valueOf(AdapterExtractorContext.getInstance()
				.getGlobalTime()));
		// 替换任务实例ID
		return tmpSql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());
	}

	public void delete() {
		String sql = super.getSql("DELETE_AUDIT_METADATA");
		sql = sql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());

		super.getJdbcTemplate().execute(sql);
	}

}
