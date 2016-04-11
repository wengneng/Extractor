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
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryCompositionDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

public class HistoryCompositionDaoImpl extends DaoBaseServiceImpl implements IHistoryCompositionDao {

	private Log log = LogFactory.getLog(HistoryCompositionDaoImpl.class);

	private String historyCompositionSql;

	private int count;

	private StringBuilder childMetadataIds = new StringBuilder();

	public void batchLoadCreate(AppendMetadata aMetadata) {
		try {
			historyCompositionSql = super.getSql("LOAD_HISTORY_CREATE_COMPOSITION");

			long cntTime = AdapterExtractorContext.getInstance().getGlobalTime();
			setCntTimeInSql(cntTime);
			String startTime = DateUtil.getFormatTime(cntTime, DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧:先写历史表
			batchLoadCreate(aMetadata.getChildMetaModels());
			String logMsg = new StringBuilder("将需删除的组合关系写入至历史元数据组合关系记录数:").append(count).append(",开始时间:").append(
					startTime).append(",结束时间:").append(
					DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
			log.info(logMsg);

			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

		}
		catch (Exception e) {
			// 删除元数据组合关系失败，此时历史组合关系可能已经把待删除的数据写进去，此时要求回滚历史组合关系
			// AdapterExtractorContext.getInstance().setCallbackHistoryOperation(true);
			rollback();

		}
		finally {
			deleteClear();
		}

	}

	public void rollback() {
		String compositionSql = super.getSql("ROLLBACK_HISTORY_COMPOSITION");
		// compositionSql = compositionSql.replaceAll(Constants.SQL_END_TIME,
		// Long.toString(AdapterExtractorContext
		// .getInstance().getGlobalTime()));

		super.getJdbcTemplate().update(compositionSql,
				new Object[] { Long.toString(AdapterExtractorContext.getInstance().getGlobalTime()) });
	}

	private void deleteClear() {
		super.clear();
		historyCompositionSql = "";
		childMetadataIds = new StringBuilder();
		count = 0;
	}

	private void batchLoadCreate(Set<MMMetaModel> metaModels) {
		for (Iterator<MMMetaModel> iter = metaModels.iterator(); iter.hasNext();) {
			MMMetaModel metaModel = iter.next();
			if (metaModel.isHasMetadata()) {
				// 存在该模型的元数据
				batchLoadCreate(metaModel.getMetadatas());
			}
			if (metaModel.isHasChildMetaModel()) {
				// 存在子元模型
				batchLoadCreate(metaModel.getChildMetaModels());
			}
		}

		if (count % Constants.MAX_EXPRESSION_SIZE != 0) {
			super.getJdbcTemplate().execute(getHistoryFullSql(childMetadataIds));
		}
	}

	private void batchLoadCreate(List<AbstractMetadata> metadatas) {
		for (AbstractMetadata metadata : metadatas) {
			if (++count % Constants.MAX_EXPRESSION_SIZE == 0) {
				super.getJdbcTemplate().execute(getHistoryFullSql(childMetadataIds));
				childMetadataIds = new StringBuilder();
			}
			childMetadataIds.append(",'").append(metadata.getId()).append("'");
		}
	}

	private String getHistoryFullSql(StringBuilder childMetadataIds) {
		return historyCompositionSql.replaceAll(Constants.SQL_INSTANCE_ID, childMetadataIds.toString());
	}

	private void setCntTimeInSql(long cntTime) {
		historyCompositionSql = historyCompositionSql.replaceAll(Constants.SQL_END_TIME, String.valueOf(cntTime));

	}

}
