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
import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.AuditMetadataMapper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.CreateHarvestMetadataHelper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.ModifyHarvestMetadataHelper;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

/**
 * 审核元数据操作DAO实现类
 * 
 * @author user
 * @version 1.0 Date: Oct 4, 2009
 * 
 */
public class HarvestMetadataDaoImpl extends DaoBaseServiceImpl implements IHarvestMetadataDao {

	private Log log = LogFactory.getLog(HarvestMetadataDaoImpl.class);

	private String sql;

	private int count;

	protected String getTaskInstanceIdSQL() {
		String taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();
		return "'" + taskInstanceId + "'";
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata,
	 *      java.lang.String)
	 */
	public void batchLoadCreate(AppendMetadata aMetadata) {
		try {
			sql = super.getSql("LOAD_CREATE_HARVEST_METADATA");

			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = aMetadata.getChildMetaModels();
			helper = new CreateHarvestMetadataHelper(super.getBatchSize());
			for (MMMetaModel metaModel : metaModels) {
				batchLoadCreate(metaModel);
			}

			count = helper.getCount();
			String logMsg = new StringBuilder("添加审核元数据记录数:").append(count).append(",开始时间:").append(startTime).append(
					",结束时间:").append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT))
					.toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

		}
		finally {
			createClear();
		}

	}

	private void createClear() {
		super.clear();
		sql = null;
		count = 0;

	}

	// 顶级结点,
	private void batchLoadCreate(final MMMetaModel metaModel) {

		if (metaModel.isHasMetadata()) {
			// 存在该模型的元数据
			String fullSql = super.genFullSql(sql, metaModel);
			((CreateHarvestMetadataHelper) helper).setMetaModel(metaModel);
			// 1表示增加
			((CreateHarvestMetadataHelper) helper).setOperType("1");
			super.getJdbcTemplate().execute(fullSql, helper);
		}

		if (metaModel.isHasChildMetaModel()) {
			// 存在子元模型
			Set<MMMetaModel> childMetaModels = metaModel.getChildMetaModels();
			for (MMMetaModel childMetaModel : childMetaModels) {
				batchLoadCreate(childMetaModel);
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao#batchLoadModify(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadModify(AppendMetadata modifyAMetadata) {
		// 修改审核元数据,采用的是添加审核元数据相同的SQL
		try {
			sql = super.getSql("LOAD_MODIFY_HARVEST_METADATA");

			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = modifyAMetadata.getChildMetaModels();
			helper = new ModifyHarvestMetadataHelper(super.getBatchSize());
			
			// 3表示修改
			((ModifyHarvestMetadataHelper) helper).setOperType("3");
			
			for (MMMetaModel metaModel : metaModels) {
				batchLoadModify(metaModel);
			}

			count = helper.getCount();
			String logMsg = new StringBuilder("添加修改审核元数据记录数:").append(count).append(",开始时间:").append(startTime).append(
					",结束时间:").append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT))
					.toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		finally {
			createClear();
		}
	}

	// 顶级结点,
	private void batchLoadModify(final MMMetaModel metaModel) {
		if (metaModel.isHasMetadata()) {
			// 存在该模型的元数据
			String mAttrSql = genFullSql(sql, metaModel);
			((ModifyHarvestMetadataHelper) helper).setMetaModel(metaModel);
			super.getJdbcTemplate().execute(mAttrSql, helper);

		}

		if (metaModel.isHasChildMetaModel()) {
			// 存在子元模型
			Set<MMMetaModel> childMetaModels = metaModel.getChildMetaModels();
			for (MMMetaModel childMetaModel : childMetaModels) {
				batchLoadModify(childMetaModel);
			}
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao#batchLoadDelete(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadDelete(AppendMetadata deleteAMetadata) {
		try {
			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);

			sql = super.getSql("LOAD_DELETE_HARVEST_METADATA");
			Set<MMMetaModel> metaModels = deleteAMetadata.getChildMetaModels();

			for (MMMetaModel metaModel : metaModels) {
				if (metaModel.isHasMetadata()) {
					// 该模型有元数据
					String modelSql = setAttrInSql(metaModel);

					List<AbstractMetadata> deleteMetadatas = metaModel.getMetadatas();
					batchLoadDelete(modelSql, deleteMetadatas);

				}
			}

			String logMsg = new StringBuilder("添加待删除元数据到审核表记录数:").append(count)
					.append(",开始时间:").append(startTime).append(",结束时间:").append(
					DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		finally {
			count = 0;
		}

	}

	/**
	 * 受SQL表达式中IN参数数据量最大为1000的限制,需多次提交
	 * 
	 * @param modelSql
	 * @param deleteMetadatas
	 */
	private void batchLoadDelete(String modelSql, List<AbstractMetadata> deleteMetadatas) {
		StringBuilder sb = new StringBuilder();
		int size = deleteMetadatas.size();
		
		for (int index = 0; index < size; count++) {
			sb.append(",'").append(deleteMetadatas.get(index).getId()).append("'");
			if (++index % Constants.MAX_EXPRESSION_SIZE == 0) {
				super.getJdbcTemplate().execute(getFullSql(modelSql, sb));
				sb = new StringBuilder();
			}
		}
		// 提交尾数,如数据量不为空,
		if (size % Constants.MAX_EXPRESSION_SIZE != 0) {
			super.getJdbcTemplate().execute(getFullSql(modelSql, sb));
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
	private String getFullSql(String sql, StringBuilder sb) {
		String tmpSql = sql.replaceAll(Constants.SQL_TASK_INSTANCE_ID, getTaskInstanceIdSQL());
		return tmpSql.replaceAll(Constants.SQL_INSTANCE_ID, sb.toString());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao#genAuditAMetadata()
	 */
	public List<AuditMetadata> genAuditAMetadata() {
		// 获取待审核表中的元数据
		String sql = GenSqlUtil.getSql("QUERY_AUDIT_METADATA");
		return super.getJdbcTemplate().query(sql,
				new Object[] { AdapterExtractorContext.getInstance().getTaskInstanceId() }, new AuditMetadataMapper());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IHarvestMetadataDao#updateOldDatas()
	 */
	public void updateOldDatas() {
		String sql = GenSqlUtil.getSql("UPDATE_OLD_AUDIT_METADATA");

		int updateSize = super.getJdbcTemplate().update(
				sql,
				new Object[] { AdapterExtractorContext.getInstance().getTaskInstanceId(),
						AdapterExtractorContext.getInstance().getTaskInstanceId(),
						AdapterExtractorContext.getInstance().getTaskInstanceId() });

		log.info("更新旧有待审核元数据的状态数:" + updateSize);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "更新旧有待审核元数据的状态数:" + updateSize);

	}
}
