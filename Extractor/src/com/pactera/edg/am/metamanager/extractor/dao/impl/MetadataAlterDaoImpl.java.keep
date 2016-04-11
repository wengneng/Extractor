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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.core.util.DateUtil;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataAlterDao;
import com.pactera.edg.am.metamanager.extractor.dao.ISequenceDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.CreateMetadataAlterHelper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.DeleteMetadataAlterHelper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.ModifyMetadataAlterHelper;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 变更信息表操作DAO的实现类(不加事务控制)
 * 
 * @author user
 * @version 1.0 Date: Oct 9, 2009
 * 
 */
public class MetadataAlterDaoImpl extends DaoBaseServiceImpl implements IMetadataAlterDao {

	private Log log = LogFactory.getLog(MetadataAlterDaoImpl.class);

	private ISequenceDao sequenceDao;

	public void setSequenceDao(ISequenceDao sequenceDao) {
		this.sequenceDao = sequenceDao;
	}

	/**
	 * 插入元数据变更信息表的SQL
	 * @return SQL
	 */
	public String getInsertAlterationInfoSql() {
		return super.getSql("INSERT_INTO_ALTERATION_INFO_SQL");
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataAlterDao#alterBatchCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void alterBatchCreate(AppendMetadata createAMetadata) {
		try {
			String sql = this.getInsertAlterationInfoSql();
			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = createAMetadata.getChildMetaModels();

			helper = new CreateMetadataAlterHelper(super.getBatchSize());
			((CreateMetadataAlterHelper) helper).setMetaModels(metaModels);
			((CreateMetadataAlterHelper) helper).setSequenceDao(sequenceDao);
			super.getJdbcTemplate().execute(sql, helper);

			String logMsg = new StringBuilder("变更信息表中记录添加的元数据记录数:").append(helper.getCount()).append(",开始时间:").append(
					startTime).append(",结束时间:").append(
					DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

		}
		catch (Exception e) {
			// 将需添加的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续
			log.error("将需添加的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"将需添加的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续");
			// 本意是期望回滚需添加的元数据在变更信息表中的记录,但该操作即把本次操作的所有变更信息都回滚了...
			log.info("回滚变更信息记录...");
			rollback();
			log.info("回滚变更信息记录完成!");
		}
		finally {
			super.clear();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataAlterDao#alterBatchDelete(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void alterBatchDelete(AppendMetadata deleteAMetadata) {
		try {
			String sql = super.getSql("DELETE_INTO_ALTERATION_INFO_SQL");;
			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = deleteAMetadata.getChildMetaModels();

			helper = new DeleteMetadataAlterHelper(super.getBatchSize());
			((DeleteMetadataAlterHelper) helper).setMetaModels(metaModels);
			((DeleteMetadataAlterHelper) helper).setSequenceDao(sequenceDao);
			super.getJdbcTemplate().execute(sql, helper);

			String logMsg = new StringBuilder("变更信息表中记录删除的元数据记录数:").append(helper.getCount()).append(",开始时间:").append(
					startTime).append(",结束时间:").append(
					DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
			
		}
		catch (Exception e) {
			// 将需删除的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续
			log.error("将需删除的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"将需删除的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续");
			// 本意是期望回滚需删除的元数据在变更信息表中的记录,但该操作即把本次操作的所有变更信息都回滚了...
			log.info("回滚变更信息记录...");
			rollback();
			log.info("回滚变更信息记录完成!");
		}

		finally {
			super.clear();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataAlterDao#alterBatchModify(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void alterBatchModify(AppendMetadata modifyAMetadata) {
		try {
			String sql = this.getInsertAlterationInfoSql();
			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = modifyAMetadata.getChildMetaModels();

			helper = new ModifyMetadataAlterHelper(super.getBatchSize());
			((ModifyMetadataAlterHelper) helper).setMetaModels(metaModels);
			((ModifyMetadataAlterHelper) helper).setSequenceDao(sequenceDao);
			super.getJdbcTemplate().execute(sql, helper);

			String logMsg = new StringBuilder("变更信息表中记录修改的元数据记录数:").append(helper.getCount()).append(",开始时间:").append(
					startTime).append(",结束时间:").append(
					DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

		}
		catch (Exception e) {
			// 将需修改的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续
			log.error("将需修改的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"将需修改的元数据写入至变更信息表时发生异常，将该异常捕获之，并不影响下一步操作的继续");
			// 本意是期望回滚需修改的元数据在变更信息表中的记录,但该操作即把本次操作的所有变更信息都回滚了...
			log.info("回滚变更信息记录...");
			rollback();
			log.info("回滚变更信息记录完成!");
		}
		finally {
			super.clear();
		}

	}

	/**
	 * 回滚已写入至元数据变更记录表的数据,发生异常的话,不管它,不会影响到下一次的入库操作
	 */
	public void rollback() {
		try {
			delete();
		}
		catch (Exception e) {
			// 回滚数据发生异常,因其为变更信息表的回滚,内部消化掉
			log.error("回滚变更信息表的数据发生异常,将该异常捕获之，并不影响下一步操作的继续", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "回滚变更信息表的数据发生异常,将该异常捕获之，并不影响下一步操作的继续");
		}
	}

	private void delete() {
		String sql = super.getSql("DELETE_ALTER_METADATA");
		int size = super.getJdbcTemplate().update(sql,
				new Object[] { String.valueOf(AdapterExtractorContext.getInstance().getGlobalTime()) });
		
		log.info("回滚变更信息表成功!,回滚记录数:" + size);
	}

}
