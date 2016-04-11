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
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.core.util.DateUtil;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryMetadataDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

/**
 * 历史元数据dao层实现类
 * 
 * @author user
 * @version 1.0 Date: Jul 9, 2009
 * 
 */
public class HistoryMetadataDaoImpl extends DaoBaseServiceImpl implements IHistoryMetadataDao {

	private Log log = LogFactory.getLog(HistoryMetadataDaoImpl.class);

	protected String sql;

	protected int count;

	protected void batchLoadCreate(MMMetaModel metaModel) {
		if (metaModel.isHasMetadata()) {
			// 存在该模型的元数据
			String fullSql = setAttrInSql(metaModel);
			batchLoadCreate(metaModel.getMetadatas(), fullSql);
		}

		if (metaModel.isHasChildMetaModel()) {
			// 存在子元模型
			Set<MMMetaModel> childMetaModels = metaModel.getChildMetaModels();
			for (MMMetaModel childMetaModel : childMetaModels) {
				batchLoadCreate(childMetaModel);
			}
		}

	}

	private void batchLoadCreate(List<AbstractMetadata> metadatas, String fullSql) {
		StringBuilder sb = new StringBuilder();

		for (AbstractMetadata metadata : metadatas) {
			sb.append(",'").append(metadata.getId()).append("'");
			if (++count % Constants.MAX_EXPRESSION_SIZE == 0) {
				super.getJdbcTemplate().execute(getFullSql(fullSql, sb));
				sb = new StringBuilder();
			}
		}

		// 提交尾数,如数据量不为空,
		if (count % Constants.MAX_EXPRESSION_SIZE != 0) {
			super.getJdbcTemplate().execute(getFullSql(fullSql, sb));
		}

	}

	/**
	 * 将元模型的必有属性都替换至SQL中
	 * 
	 * @param metaModel
	 * @return
	 */
	private String setAttrInSql(MMMetaModel metaModel) {
		Map<String, String> mAttrs = metaModel.getMAttrs();
		StringBuilder mAttrsString = new StringBuilder();
		for (Iterator<String> iter = mAttrs.values().iterator(); iter.hasNext();) {
			mAttrsString.append(",").append(iter.next());
		}
		return sql.replaceAll(Constants.SQL_ATTR_KEYS_FLAG, mAttrsString.toString());
	}

	private void setCommonInSql(String type, long cntTime) {
		sql = sql.replaceAll(Constants.SQL_OPER_TYPE, type);
		sql = sql.replaceAll(Constants.SQL_END_TIME, String.valueOf(cntTime));

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
	private String getFullSql(String sql, StringBuilder sb) {

		return sql.replaceAll(Constants.SQL_INSTANCE_ID, sb.toString());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHistoryMetadataDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata,
	 *      java.lang.String)
	 */
	public void batchLoadCreate(AppendMetadata modifyAMetadata, String type) {

		String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
		sql = super.getSql("LOAD_HISTORY_CREATE_METADATA");
		count = 0;
		long cntTime = AdapterExtractorContext.getInstance().getGlobalTime();
		setCommonInSql(type, cntTime);
		Set<MMMetaModel> metaModels = modifyAMetadata.getChildMetaModels();
		try {
			for (MMMetaModel metaModel : metaModels) {
				batchLoadCreate(metaModel);
			}
			String logMsg = new StringBuilder("添加历史元数据记录数:").append(count).append(",开始时间:").append(startTime).append(
					",结束时间:").append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT))
					.toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		catch (Exception e) {
			// 元数据信息添加至历史表时，发生异常，将其捕获，不影响下一步的进行，但要求将部分已写至历史表的信息回滚
			log.error("元数据信息添加至历史表时，发生异常，将其捕获，不影响下一步的进行，但要求将部分已写至历史表的信息回滚", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"元数据信息添加至历史表时，发生异常，将其捕获，不影响下一步的进行，但要求将部分已写至历史表的信息回滚");
			/**
			 * 两种策略:1.回滚写入至历史元数据表的修改的/删除的元数据;
			 * 2.回滚所有本次操作的数据,包括:已修改的历史元数据(历史元数据表);已添加的依赖关系(依赖关系表);已添加的组合关系(组合关系表);
			 * 已添加的元数据变更信息(变更信息表);已添加的元数据(元数据表);
			 * 但此处将本次写入至历史元数据表的记录都回滚,貌似不够精确....
			 */
			rollback();
		}
	}

	public void rollback() {

		String instanceSql = GenSqlUtil.getSql("ROLLBACK_HISTORY_INSTANCE");
		// instanceSql = instanceSql.replaceAll(Constants.SQL_END_TIME,
		// Long.toString(AdapterExtractorContext
		// .getInstance().getGlobalTime()));

		int size = super.getJdbcTemplate().update(instanceSql,
				new Object[] { Long.toString(AdapterExtractorContext.getInstance().getGlobalTime()) });
		log.info("回滚历史元数据记录成功,回滚记录数:" + size);
	}
}
