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

package com.pactera.edg.am.metamanager.extractor.dao.helper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.core.util.DateUtil;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 添加元数据组合关系操作PreparedStatementCallback辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Aug 21, 2009
 */
public class CreateCompositionHelper extends PreparedStatementCallbackHelper {

	private Log log = LogFactory.getLog(CreateCompositionHelper.class);

	private Set<MMMetaModel> metaModels;

	public CreateCompositionHelper(int batchSize)
	{
		super(batchSize);
	}

	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException {
		String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);

		for (Iterator<MMMetaModel> iter = metaModels.iterator(); iter.hasNext();) {
			// 遍历元模型
			batchLoadCreate(iter.next(), ps);
		}
		// 遍历完毕,仍需提交尾数

		if (super.count % super.batchSize != 0) {
			ps.executeBatch();
			ps.clearBatch();

		}
		
		String logMsg = new StringBuilder(getMsg()).append(count).append(",开始时间:").append(startTime).append(",结束时间:")
				.append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
		log.info(logMsg);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

		return null;
		// test for callback
		// throw new SQLException();
	}
	
	protected String getMsg(){
		return "添加元数据组合关系记录数:";
	}

	private void batchLoadCreate(final MMMetaModel metaModel, PreparedStatement ps) throws SQLException {

		if (metaModel.isHasMetadata()) {
			// 存在该模型的元数据
			doInPreparedStatement(metaModel, ps);
		}

		if (metaModel.isHasChildMetaModel()) {
			// 存在子元模型
			Set<MMMetaModel> childMetaModels = metaModel.getChildMetaModels();
			for (MMMetaModel childMetaModel : childMetaModels) {
				batchLoadCreate(childMetaModel, ps);
			}
		}

	}

	private void doInPreparedStatement(MMMetaModel metaModel, PreparedStatement ps) throws SQLException {
		List<AbstractMetadata> metadatas = metaModel.getMetadatas();
		String parentMetaModelId = metaModel.getParentMetaModel().getCode();
		String metaModelId = metaModel.getCode();
		String relationshipName = metaModel.getCompedRelationCode();
		long sysTime = AdapterExtractorContext.getInstance().getGlobalTime();

		for (int i = 0, size = metadatas.size(); i < size; i++) {

			AbstractMetadata metadata = metadatas.get(i);
			if (metadata.isHasExist()) {
				// 该结点已经存在于元数据库中,则不需插入该组合关系记录
				continue;
			}

			// 元数据父结点ID
			ps.setString(1, metadata.getParentMetadata().getId());
			// 元数据父结点模型ID
			ps.setString(2, parentMetaModelId);
			// 元数据ID
			ps.setString(3, metadata.getId());
			// 元数据模型ID
			ps.setString(4, metaModelId);
			// 组合关系名称
			ps.setString(5, relationshipName);
			// 系统时间
			ps.setLong(6, sysTime);

			setPs(ps, 6);

			ps.addBatch();
			ps.clearParameters();

			if (++super.count % super.batchSize == 0) {
				ps.executeBatch();
				ps.clearBatch();
			}
		}

	}

	protected void setPs(PreparedStatement ps, int index) throws SQLException {

	}

	public void setMetaModels(Set<MMMetaModel> metaModels) {
		this.metaModels = metaModels;
	}
}
