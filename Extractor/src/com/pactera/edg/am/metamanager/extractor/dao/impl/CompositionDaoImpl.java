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
import com.pactera.edg.am.metamanager.extractor.dao.ICompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryCompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataAlterDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.CreateCompositionHelper;
import com.pactera.edg.am.metamanager.extractor.ex.DataRollbackException;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;

/**
 * 组合关系库表操作的DAO实现类
 * 
 * @author user
 * @version 1.0 Date: Jul 23, 2009
 * 
 */
public class CompositionDaoImpl extends DaoBaseServiceImpl implements ICompositionDao {

	private Log log = LogFactory.getLog(CompositionDaoImpl.class);

	private String compositionSql;

	private StringBuilder childMetadataIds = new StringBuilder();

	private int count;

	private void createClear() {
		super.clear();
		compositionSql = "";
	}

	private void deleteClear() {
		createClear();
		childMetadataIds = new StringBuilder();
		count = 0;
	}

	private void batchLoadDelete(Set<MMMetaModel> metaModels) {
		for (Iterator<MMMetaModel> iter = metaModels.iterator(); iter.hasNext();) {
			MMMetaModel metaModel = iter.next();
			if (metaModel.isHasMetadata()) {
				// 存在该模型的元数据
				batchLoadDeleteComposition(metaModel.getMetadatas());
			}

			if (metaModel.isHasChildMetaModel()) {
				// 存在子元模型
				batchLoadDelete(metaModel.getChildMetaModels());
			}
		}

		if (count % Constants.MAX_EXPRESSION_SIZE != 0) {

			// 提交尾数,如数据量不为空,
			super.getJdbcTemplate().execute(getFullSql(childMetadataIds));
		}

	}

	private void batchLoadDeleteComposition(List<AbstractMetadata> metadatas) {
		for (AbstractMetadata metadata : metadatas) {
			// TODO 有问题??
			if (++count % Constants.MAX_EXPRESSION_SIZE == 0) {
				super.getJdbcTemplate().execute(getFullSql(childMetadataIds));
				childMetadataIds = new StringBuilder();
			}
			childMetadataIds.append(",'").append(metadata.getId()).append("'");
		}
	}

	private String getFullSql(StringBuilder childMetadataIds) {
		return compositionSql.replaceAll(Constants.SQL_INSTANCE_ID, childMetadataIds.toString());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.ICompositionDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadCreate(AppendMetadata createAMetadata) throws Exception, DataRollbackException {
		try {
			compositionSql = super.getSql("LOAD_CREATE_COMPOSITION");

			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = createAMetadata.getChildMetaModels();

			helper = new CreateCompositionHelper(super.getBatchSize());
			((CreateCompositionHelper) helper).setMetaModels(metaModels);
			super.getJdbcTemplate().execute(compositionSql, helper);
			
		}
		catch (Exception e) {
			// 批量添加组合关系发生异常,要求回滚数据,包括组合关系,元数据变更记录,元数据
			log.warn("添加元数据组合关系发生异常!回滚数据,包括回滚组合关系,元数据变更记录,元数据");
			rollback(createAMetadata);
			log.info("数据回滚成功!");
			throw e;

		}
		finally {
			createClear();
		}

	}

	private void rollback(AppendMetadata createAMetadata) throws DataRollbackException {
		try {
			rollbackComposition();
			
			IMetadataAlterDao metadataAlterDao = (IMetadataAlterDao) ExtractorContextLoader
					.getBean(IMetadataAlterDao.SPRING_NAME);
			metadataAlterDao.rollback();
			IMetadataDao metadataDao = (IMetadataDao) ExtractorContextLoader.getBean(IMetadataDao.SPRING_NAME);
			metadataDao.rollback(createAMetadata);
		}
		catch (Exception e) {
			throw new DataRollbackException("回滚已添加的元数据组合关系发生异常", e);
		}
	}
	
	private void rollbackComposition(){
		String compositionSql = super.getSql("ROLLBACK_MD_COMPOSITION");

		int size = super.getJdbcTemplate().update(compositionSql,
				new Object[] { Long.toString(AdapterExtractorContext.getInstance().getGlobalTime()) });
		
		log.info("组合关系回滚成功!回滚记录数:" + size);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.ICompositionDao#batchLoadDelete(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadDelete(AppendMetadata deleteAMetadata) {
		try {
			compositionSql = super.getSql("LOAD_DELETE_COMPOSITION");
			String startTime = DateUtil.getFormatTime(AdapterExtractorContext.getInstance().getGlobalTime(),
					DateUtil.DATA_FORMAT);

			// 然后删除组合关系
			batchLoadDelete(deleteAMetadata.getChildMetaModels());

			String logMsg = new StringBuilder("删除元数据组合关系记录数:").append(count).append(",开始时间:").append(startTime).append(
					",结束时间:").append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT))
					.toString();
			log.info(logMsg);

			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		catch (Exception e) {
			// 删除元数据组合关系失败，此时历史组合关系可能已经把待删除的数据写进去，此时要求回滚历史组合关系
			// AdapterExtractorContext.getInstance().setCallbackHistoryOperation(true);
			rollback();

			// 但是组合关系被删除的没有回滚,怎么办?
			throw new RuntimeException(e);
		}
		finally {
			deleteClear();
		}

	}

	private void rollback() {
		IHistoryCompositionDao historyCompositionDao = (IHistoryCompositionDao) ExtractorContextLoader
				.getBean(IHistoryCompositionDao.SPRING_NAME);
		historyCompositionDao.rollback();

	}

}
