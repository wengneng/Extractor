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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.ISequenceDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 需删除的元数据,写至变更信息表时的PreparedStatementCallback辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Oct 10, 2009
 */
public class DeleteMetadataAlterHelper extends PreparedStatementCallbackHelper {

	private Log log = LogFactory.getLog(CreateMetadataAlterHelper.class);

	protected Set<MMMetaModel> metaModels;

	protected ISequenceDao sequenceDao;

	protected String taskInstanceId;

	// 用户ID
	protected String userId;

	protected long startTime;

	/**
	 * 存放元数据ID与命名空间的键值对,key:元数据ID,value:父结点namespace
	 */
	protected Map<String, String> namespaceCache = new HashMap<String, String>();

	public DeleteMetadataAlterHelper(int batchSize)
	{
		super(batchSize);
		userId = AdapterExtractorContext.getInstance().getUserId();

	}

	/**
	 * 注入ID的生成类
	 * 
	 * @param sequenceDao
	 */
	public void setSequenceDao(ISequenceDao sequenceDao) {
		this.sequenceDao = sequenceDao;
	}

	public void setMetaModels(Set<MMMetaModel> metaModels) {
		this.metaModels = metaModels;
	}

	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
		startTime = AdapterExtractorContext.getInstance().getGlobalTime();
		taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();

		for (MMMetaModel metaModel : metaModels) {
			batchLoadDelete(ps, metaModel);
		}

		if (super.count % super.batchSize != 0) {
			ps.executeBatch();
			ps.clearBatch();

		}
		return null;
	}

	private void batchLoadDelete(PreparedStatement ps, MMMetaModel metaModel) throws SQLException {
		if (metaModel.isHasMetadata()) {
			// 有添加的元数据
			List<AbstractMetadata> metadatas = metaModel.getMetadatas();
			doInPreparedStatement(ps, metaModel.getCode(), metaModel.isHasChildMetaModel(), metadatas);
		}

		if (metaModel.isHasChildMetaModel()) {
			// 还有子元模型
			for (MMMetaModel childMetaModel : metaModel.getChildMetaModels()) {
				batchLoadDelete(ps, childMetaModel);
			}
		}

	}

	protected void doInPreparedStatement(PreparedStatement ps, String metaModelCode, boolean hasChildMetaModel,
			List<AbstractMetadata> metadatas) throws SQLException {
		try {
			for (AbstractMetadata metadata : metadatas) {
				// 变更ID
				String sequenceId = sequenceDao.getUuid();
				ps.setString(1, sequenceId);
				// 变更类型,删除为1
				ps.setString(2, "1");
				// 任务实例ID
				ps.setString(3, taskInstanceId);
				// // 元数据ID
				// ps.setString(4, metadata.getId());
				// // 元模型
				// ps.setString(5, metaModelCode);
				// 用户ID
				ps.setString(4, userId);

				// START_TIME：元数据的START_TIME
				ps.setLong(5, metadata.getStartTime());
				// 更新时间: ALTERATION_TIME
				ps.setLong(6, startTime);

				// OLD_START_TIME： 删除元数据时，变更信息表的OLD_START_TIME不需要记录
				ps.setNull(7, java.sql.Types.BIGINT);
				// 元数据ID
				ps.setString(8, metadata.getId());

				ps.addBatch();
				ps.clearParameters();

				if (++super.count % super.batchSize == 0) {
					ps.executeBatch();
					ps.clearBatch();
				}

			}
		}
		catch (SQLException e) {
			// 变更信息,非重要信息,当出现异常时,捕获之并且不抛出
			log.warn("写入变更信息表出现异常!", e);
		}

	}
}
