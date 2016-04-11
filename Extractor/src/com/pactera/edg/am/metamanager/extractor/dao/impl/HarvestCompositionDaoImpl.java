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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.core.util.DateUtil;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestCompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.CreateHarvestCompositionHelper;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

/**
 * 审核元数据组合关系操作DAO实现类
 * 
 * @author user
 * @version 1.0 Date: Oct 5, 2009
 * 
 */
public class HarvestCompositionDaoImpl extends DaoBaseServiceImpl implements IHarvestCompositionDao {

	private Log log = LogFactory.getLog(HarvestCompositionDaoImpl.class);

	private StringBuilder sb = new StringBuilder();

	private int count;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestCompositionDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadCreate(AppendMetadata createAMetadata) {
		// 写入待审核表
		try {
			String compositionSql = super.getSql("LOAD_CREATE_HARVEST_COMPOSITION");

			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = createAMetadata.getChildMetaModels();

			helper = new CreateHarvestCompositionHelper(super.getBatchSize());
			((CreateHarvestCompositionHelper) helper).setMetaModels(metaModels);
			super.getJdbcTemplate().execute(compositionSql, helper);
		}
		finally {
			super.clear();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestCompositionDao#batchLoadDelete(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadDelete(AppendMetadata deleteAMetadata) {
		try {
			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);

			String sql = super.getSql("LOAD_DELETE_HARVEST_COMPOSITION");
			Set<MMMetaModel> metaModels = deleteAMetadata.getChildMetaModels();

			for (MMMetaModel metaModel : metaModels) {
				if (metaModel.isHasMetadata()) {
					// 该模型有元数据

					List<AbstractMetadata> deleteMetadatas = metaModel.getMetadatas();
					batchLoadDelete(sql, deleteMetadatas);

				}
			}
			// 提交尾数,如数据量不被最大数整除，则提交尾数,
			if (count % Constants.MAX_EXPRESSION_SIZE != 0) {
				super.getJdbcTemplate().execute(getFullSql(sql));
			}

			String logMsg = new StringBuilder("添加待删除组合关系到审核表记录数:").append(count)
					.append(",开始时间:").append(startTime).append(",结束时间:").append(
					DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		finally {
			count = 0;
			sb = new StringBuilder();
		}

	}

	private void batchLoadDelete(String sql, List<AbstractMetadata> deleteMetadatas) {
		int size = deleteMetadatas.size();
		for (int i = 0; i < size; i++) {
			sb.append(",'").append(deleteMetadatas.get(i).getId()).append("'");
			if (++count % Constants.MAX_EXPRESSION_SIZE == 0) {
				super.getJdbcTemplate().execute(getFullSql(sql));
				sb = new StringBuilder();
			}
		}

	}

	/**
	 * 拼装成完整的SQL
	 * 
	 * @param sql
	 *            待替换的SQL
	 * @param sb
	 *            元数据ID列表
	 * @return 返回拼装好的SQL,但不改变原来的SQL
	 */
	private String getFullSql(String sql) {
		String tmpSql = sql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());
		return tmpSql.replaceAll(Constants.SQL_INSTANCE_ID, sb.toString());
	}
	
	protected String getTaskInstanceIdSQL() {
		String taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();
		return "'" + taskInstanceId + "'";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestCompositionDao#updateOldDatas()
	 */
	public void updateOldDatas() {
		String sql = GenSqlUtil.getSql("UPDATE_OLD_AUDIT_COMPOSITION");

		int updateSize = super.getJdbcTemplate().update(
				sql,
				new Object[] { AdapterExtractorContext.getInstance().getTaskInstanceId(),
						AdapterExtractorContext.getInstance().getTaskInstanceId(),
						AdapterExtractorContext.getInstance().getTaskInstanceId() });

		log.info("更新旧有待审核组合关系的状态数:" + updateSize);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "更新旧有待审核组合关系的状态数:" + updateSize);

	}

}
