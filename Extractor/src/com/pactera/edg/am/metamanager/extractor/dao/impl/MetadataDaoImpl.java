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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.core.util.DateUtil;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryCompositionDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryDependencyDao;
import com.pactera.edg.am.metamanager.extractor.dao.IHistoryMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataAlterDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.AppendMetadataMapper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.CreateMetadataHelper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.MetadataMapper;
import com.pactera.edg.am.metamanager.extractor.dao.helper.MetadataMapper3;
import com.pactera.edg.am.metamanager.extractor.dao.helper.ModifyMetadataHelper;
import com.pactera.edg.am.metamanager.extractor.ex.DataRollbackException;
import com.pactera.edg.am.metamanager.extractor.ex.MetadataNotFoundException;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;

/**
 * 元数据dao层实现类
 * 
 * @author hqchen
 * @version 1.0 Date: Jul 9, 2009
 */
public class MetadataDaoImpl extends DaoBaseServiceImpl implements IMetadataDao {

	private Log log = LogFactory.getLog(MetadataDaoImpl.class);

	private String sql;

	private int count;

	private StringBuilder metadataIds = new StringBuilder();;

	private void createClear() {
		super.clear();
		count = 0;
		sql = "";

	}

	private void deleteClear() {
		createClear();
		metadataIds = new StringBuilder();

	}

	private void batchLoadDelete(MMMetaModel metaModel) {
		if (metaModel.isHasMetadata()) {
			// 存在该模型的元数据
			batchLoadDelete(metaModel.getMetadatas());
		}

		if (metaModel.isHasChildMetaModel()) {
			// 存在子元模型
			Set<MMMetaModel> childMetaModels = metaModel.getChildMetaModels();
			for (MMMetaModel childMetaModel : childMetaModels) {
				batchLoadDelete(childMetaModel);
			}
		}

	}

	private void batchLoadDelete(List<AbstractMetadata> metadatas) {
		for (AbstractMetadata metadata : metadatas) {
			metadataIds.append(",'").append(metadata.getId()).append("'");
			if (++count % Constants.MAX_EXPRESSION_SIZE == 0) {
				String fullSql = sql.replaceAll(Constants.SQL_INSTANCE_ID, metadataIds.toString());
				super.getJdbcTemplate().execute(fullSql);
				metadataIds = new StringBuilder();
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao#queryMetadata(MMMetadata,
	 *      String, com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel)
	 */
	public MMMetadata queryMetadata(MMMetadata parentMetadata, String metadataId, MMMetaModel metaModel) {
		String sql = super.getSql("QUERY_METADATA");
		String fullSql = genFullSql(sql, metaModel);

		MMMetadata metadata = (MMMetadata) super.getJdbcTemplate().queryForObject(fullSql, new Object[] { metadataId },
				new MetadataMapper3(metaModel));

		metadata.setParentMetadata(parentMetadata);
		return metadata;
	}

	/**
	 * 获取所有父结点的指定模型的子孙结点
	 * 
	 * @param parentMetadatas
	 *            父结点的元数据列表
	 * @param childMetaModel
	 *            子结点的元模型
	 * @return
	 */
	public List<MMMetadata> queryChildMetadatas(List<AbstractMetadata> parentMetadatas, MMMetaModel childMetaModel) {
		String sql = super.getSql("QUERY_CHILD_METADATA2");
		String fullSql = genFullSql(sql, childMetaModel, parentMetadatas);

		Map<String, MMMetadata> parentMetadatasMap = getParentMetadataMap(parentMetadatas);
		return (List<MMMetadata>) super.getJdbcTemplate().query(fullSql, new Object[] { childMetaModel.getCode() },
				new MetadataMapper(childMetaModel, parentMetadatasMap));
	}

	/**
	 * 获取指定父结点的指定模型的子结点
	 * 
	 * @param parentMetadata
	 *            父结点的元数据
	 * @param childMetaModel
	 *            子结点的元模型
	 * @return
	 */
	public List<AbstractMetadata> queryChildMetadatas(MMMetadata parentMetadata, MMMetaModel childMetaModel) {
		String sql = super.getSql("QUERY_CHILD_METADATA");
		String fullSql = genFullSql(sql, childMetaModel);
		Map<String, MMMetadata> parentMetadatasMap = new HashMap<String, MMMetadata>(1);
		parentMetadatasMap.put(parentMetadata.getId(), parentMetadata);

		return (List<AbstractMetadata>) super.getJdbcTemplate().query(fullSql,
				new Object[] { parentMetadata.getId(), childMetaModel.getCode() },
				new MetadataMapper(childMetaModel, parentMetadatasMap));
	}

	private Map<String, MMMetadata> getParentMetadataMap(List<AbstractMetadata> metadatas) {
		int size = metadatas.size();
		Map<String, MMMetadata> metadatasMap = new HashMap<String, MMMetadata>(size);
		for (int i = 0; i < size; i++) {
			MMMetadata metadata = (MMMetadata) metadatas.get(i);
			metadatasMap.put(metadata.getId(), metadata);
		}
		return metadatasMap;
	}

	protected String genFullSql(String sql, MMMetaModel metaModel) {
		Map<String, String> mAttributes = metaModel.getMAttrs();
		StringBuilder keys = new StringBuilder();
		for (Iterator<String> iter = mAttributes.keySet().iterator(); iter.hasNext();) {
			String position = mAttributes.get(iter.next());
			keys.append(",m.").append(position).append(" AS ").append(position);
		}

		String fullSql = sql;
		fullSql = fullSql.replaceAll(Constants.SQL_ATTR_KEYS_FLAG, keys.toString());

		return fullSql;
	}

	private String genFullSql(String sql, MMMetaModel metaModel, List<AbstractMetadata> metadatas) {

		String fullSql = genFullSql(sql, metaModel);

		StringBuilder parentIdSB = new StringBuilder();
		for (int i = 0, size = metadatas.size(); i < size; i++) {
			parentIdSB.append(",'");
			parentIdSB.append(((MMMetadata) metadatas.get(i)).getId());
			parentIdSB.append("'");
		}
		fullSql = fullSql.replaceAll(Constants.PARENT_METADATA_ID, parentIdSB.toString());
		return fullSql;
	}

	/**
	 * 根据元数据ID,通过SQL查询获取其元模型
	 */
	public MMMetaModel queryMetaModelBySql(String metadataId) throws MetadataNotFoundException {
		String sql = super.getSql("QUERY_METAMODEL_ID");
		try {
			return (MMMetaModel) super.getJdbcTemplate().queryForObject(sql, new Object[] { metadataId },
					new MetaModelMapper());
		}
		catch (EmptyResultDataAccessException e1) {
			// 返回空记录
			throw new MetadataNotFoundException(String.valueOf(metadataId), e1);
		}

	}

	/**
	 * 元模型RM映射类
	 * 
	 */
	private class MetaModelMapper implements RowMapper {

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			MMMetaModel metaModel = new MMMetaModel();
			metaModel.setCode(rs.getString(1));
			return metaModel;
		}

	}

	/**
	 * 判断是否存在父结点ID为parentId，自身元模型code为metadata.getClassifierId(),元数据CODE为metadata.getCode()的元数据
	 * 如存在，则返回true，同时设置该元数据的ID，否则返回false
	 */
	public boolean existMetadata(String parentId, MMMetadata metadata) {
		String sql = super.getSql("QUERY_EXIST_METADATA");
		String logMsg = "";
		try {
			String namespace = (String) super.getJdbcTemplate().queryForObject(sql,
					new Object[] { parentId, metadata.getClassifierId(), metadata.getCode() }, String.class);
			metadata.setNamespace(namespace);
			metadata.setId(namespace.substring(namespace.lastIndexOf("/") + 1));
			return true;
		}
		catch (EmptyResultDataAccessException e) {
			logMsg = new StringBuilder("库表中不存在元数据:parent_id:").append(parentId).append(", classifier_id:").append(
					metadata.getClassifierId()).append(",code:").append(metadata.getCode()).toString();
			log.warn(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.WARN, logMsg);
			return false;
		}
	}

	/**
	 * 查询获取悬挂结点元数据
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao#queryAMetadata(String)
	 */
	public AppendMetadata queryAMetadata(String metadataId) {
		String logMsg;
		String sql = super.getSql("QUERY_EXIST_METADATA_2");
		try {
			return (AppendMetadata) super.getJdbcTemplate().queryForObject(sql, new Object[] { metadataId },
					new AppendMetadataMapper(metadataId));
		}
		catch (EmptyResultDataAccessException e) {
			logMsg = new StringBuilder("库表中不存在元数据:metadataId:").append(metadataId).toString();
			log.warn(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.WARN, logMsg);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao#batchLoadCreate(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadCreate(AppendMetadata createAMetadata) throws Exception, DataRollbackException {
		String logMsg;
		try {
			sql = super.getSql("LOAD_CREATE_METADATA");

			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧,遍历需要写入库表的元数据,然后批量入库
			Set<MMMetaModel> metaModels = createAMetadata.getChildMetaModels();
			helper = new CreateMetadataHelper(super.getBatchSize());
			for (MMMetaModel metaModel : metaModels) {
				batchLoadCreate(metaModel);
			}

			count = helper.getCount();
			logMsg = new StringBuilder("添加元数据记录数:").append(count).append(",开始时间:").append(startTime).append(",结束时间:")
					.append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT)).toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

		}
		catch (Exception e) {
			// 添加元数据失败，此时要求回滚已写的元数据
			// AdapterExtractorContext.getInstance().setCallbackMetadata(true);
			// 直接在此处回滚,即将已经写入的数据,让它吐出来
			log.error("写入元数据表出现异常,将回滚之...");
			count = 0;
			rollback(createAMetadata);
			log.info("回滚已写入的元数据记录完成!");
			throw e;
		}
		finally {
			createClear();
		}

	}

	/**
	 * 回滚需添加的元数据记录
	 */
	public void rollback(AppendMetadata aMetadata) throws DataRollbackException {
		try {
			batchLoadDelete1(aMetadata);
		}
		catch (Exception ee) {
			// 数据回滚出现异常
			throw new DataRollbackException("回滚已写入的元数据记录出现异常", ee);
		}
	}

	/**
	 * @param metaModel
	 */
	private void batchLoadCreate(final MMMetaModel metaModel) {

		if (metaModel.isHasMetadata()) {
			// 存在该模型的元数据
			String fullSql = super.genFullSql(sql, metaModel);
			((CreateMetadataHelper) helper).setMetaModel(metaModel);
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
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao#batchLoadModify(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadModify(AppendMetadata modifyAMetadata) throws Exception {
		try {
			sql = super.getSql("LOAD_MODIFY_METADATA");

			String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			// 由此实行抽丝剥茧
			Set<MMMetaModel> metaModels = modifyAMetadata.getChildMetaModels();
			helper = new ModifyMetadataHelper(super.getBatchSize());
			for (MMMetaModel metaModel : metaModels) {
				batchLoadModify(metaModel);
			}

			count = helper.getCount();
			String logMsg = new StringBuilder("修改元数据记录数:").append(count).append(",开始时间:").append(startTime).append(
					",结束时间:").append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT))
					.toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		catch (Exception e) {
			// 修改元数据失败，此时历史元数据表已经把待修改的数据写进去，要求回滚历史元数据
			// AdapterExtractorContext.getInstance().setCallbackHistoryOperation(true);

			log.warn("修改元数据发生异常!回滚历史元数据,变更信息元数据");
			rollbackHistoryMetadata();
			rollbackMetadataAlter();

			throw e;
		}
		finally {
			createClear();
		}

	}

	private void rollbackMetadataAlter() {
		IMetadataAlterDao metadataAlterDao = (IMetadataAlterDao) ExtractorContextLoader
				.getBean(IMetadataAlterDao.SPRING_NAME);
		metadataAlterDao.rollback();
	}

	private void rollbackHistoryMetadata() {
		IHistoryMetadataDao historyMetadataDao = (IHistoryMetadataDao) ExtractorContextLoader
				.getBean(IHistoryMetadataDao.SPRING_NAME);
		historyMetadataDao.rollback();

	}

	// 顶级结点,
	private void batchLoadModify(final MMMetaModel metaModel) {

		if (metaModel.isHasMetadata()) {
			// 存在该模型的元数据
			String fullSql = super.genModifySql(sql, metaModel);
			((ModifyMetadataHelper) helper).setMetaModel(metaModel);
			super.getJdbcTemplate().execute(fullSql, helper);
		}

		if (metaModel.isHasChildMetaModel()) {
			// 存在子元模型
			Set<MMMetaModel> childMetaModels = metaModel.getChildMetaModels();
			for (MMMetaModel childMetaModel : childMetaModels) {
				batchLoadModify(childMetaModel);
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao#genChildrenMetadatasId(java.lang.String)
	 */
	public List<MMMetadata> genChildrenMetadatasId(String aNamespace) {
		String sql = super.getSql("GEN_METADATAS_ID");
		sql = sql.replace(Constants.SQL_NAMESPACE, aNamespace);
		sql+= " and T.PARENT_ID !='0' ";
		return (List<MMMetadata>) super.getJdbcTemplate().query(sql, new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				MMMetadata metadata = new MMMetadata();
				metadata.setId(rs.getString("INSTANCE_ID"));
				metadata.setClassifierId(rs.getString("CLASSIFIER_ID"));
				metadata.setStartTime(rs.getLong("START_TIME"));
				return metadata;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao#batchLoadDelete(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void batchLoadDelete(AppendMetadata deleteAMetadata) throws Exception {
		try {
			batchLoadDelete1(deleteAMetadata);
		}
		catch (Exception e) {
			// 删除元数据失败，此时历史元数据表已经把待删除的数据写进去，此时要求回滚历史依赖关系
			// AdapterExtractorContext.getInstance().setCallbackHistoryOperation(true);
			rollback();
			throw e;
		}

	}

	private void rollback() {
		// 回滚历史元数据依赖关系
		IHistoryDependencyDao historyDependencyDao = (IHistoryDependencyDao) ExtractorContextLoader
				.getBean(IHistoryDependencyDao.SPRING_NAME);
		historyDependencyDao.rollback();
		// 回滚历史组合关系
		IHistoryCompositionDao historyCompositionDao = (IHistoryCompositionDao) ExtractorContextLoader
				.getBean(IHistoryCompositionDao.SPRING_NAME);
		historyCompositionDao.rollback();

		// 回滚历史元数据
		rollbackHistoryMetadata();

	}

	private void batchLoadDelete1(AppendMetadata deleteAMetadata) {

		sql = super.getSql("LOAD_DELETE_METADATA");

		String startTime = DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);

		Set<MMMetaModel> metaModels = deleteAMetadata.getChildMetaModels();
		try {
			for (MMMetaModel metaModel : metaModels) {
				batchLoadDelete(metaModel);
			}
			if (count % Constants.MAX_EXPRESSION_SIZE != 0) {
				// 提交尾数,如数据量不被最大数整除,则提交尾数
				String fullSql = sql.replaceAll(Constants.SQL_INSTANCE_ID, metadataIds.toString());
				super.getJdbcTemplate().execute(fullSql);
			}
			String logMsg = new StringBuilder("删除元数据记录数:").append(count).append(",开始时间:").append(startTime).append(
					",结束时间:").append(DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT))
					.toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		finally {
			deleteClear();
		}

	}

	/* (non-Javadoc)  
	  * @param id
	  * @return  
	  * @see com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao#getParId(java.lang.String)  
	  */  
	public String getParId(String id) {
		String sql = "select T.parent_id from  T_MD_INSTANCE T where T.instance_id = '"+id+"'";
		List<String> list = (List<String>) super.getJdbcTemplate().query(sql, new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				return rs.getString("parent_id");
			}
		});
		if(list.size()!=0){
			return list.get(0).toString();
		}
		return null;
	}

}
