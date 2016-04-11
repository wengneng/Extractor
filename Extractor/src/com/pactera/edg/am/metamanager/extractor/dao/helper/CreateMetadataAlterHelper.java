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
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.ISequenceDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 记录已经添加的元数据到变更信息表的辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Oct 9, 2009
 */
public class CreateMetadataAlterHelper extends PreparedStatementCallbackHelper {

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
	

	/**
	 * 注入ID的生成类
	 * @param sequenceDao
	 */
	public void setSequenceDao(ISequenceDao sequenceDao) {
		this.sequenceDao = sequenceDao;
	}


	public void setMetaModels(Set<MMMetaModel> metaModels) {
		this.metaModels = metaModels;
	}

	public CreateMetadataAlterHelper(int batchSize) {
		super(batchSize);
		userId = AdapterExtractorContext.getInstance().getUserId();
	}

	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
		startTime = AdapterExtractorContext.getInstance().getGlobalTime();
		taskInstanceId = AdapterExtractorContext.getInstance().getTaskInstanceId();
		
		for (MMMetaModel metaModel : metaModels) {
			batchLoadCreate(ps, metaModel);
		}


		if (super.count % super.batchSize != 0) {
			ps.executeBatch();
			ps.clearBatch();

		}
		return null;
	}

	private void batchLoadCreate(PreparedStatement ps, MMMetaModel metaModel) throws SQLException {
		if (metaModel.isHasMetadata()) {
			// 有添加的元数据
			List<AbstractMetadata> metadatas = metaModel.getMetadatas();
			doInPreparedStatement(ps, metaModel.getCode(), metaModel.isHasChildMetaModel(), metadatas);
		}

		if (metaModel.isHasChildMetaModel()) {
			// 还有子元模型
			for (MMMetaModel childMetaModel : metaModel.getChildMetaModels()) {
				batchLoadCreate(ps, childMetaModel);
			}
		}

	}

	protected void doInPreparedStatement(PreparedStatement ps, String metaModelCode, boolean hasChildMetaModel,
			List<AbstractMetadata> metadatas) throws SQLException {
		try{
			for (AbstractMetadata metadata : metadatas) {
				// 变更ID
				String sequenceId = sequenceDao.getUuid();
				ps.setString(1, sequenceId );
	
				// 任务实例ID
				ps.setString(3, taskInstanceId);
				// 元数据ID
				ps.setString(4, metadata.getId());
				// 元模型
				ps.setString(5, metaModelCode);
				// 用户ID
				ps.setString(7, userId);
				
				// 更新时间: ALTERATION_TIME
				ps.setLong(9, startTime);
	
				// 属性信息 （不记录属性内容了 2010-05-18 fbchen）
				//ps.setString(3, genAttrs(metadata));
				
				setPs(ps, metadata, metaModelCode, hasChildMetaModel);
	
				String parentId = metadata.getParentMetadata().getId();
				if(parentId == null || parentId.equals("")){
					parentId = "0";
				}
				ps.setString(11, parentId);
				ps.addBatch();
				ps.clearParameters();
	
				if (++super.count % super.batchSize == 0) {
					ps.executeBatch();
					ps.clearBatch();
				}
	
			}
		}catch(SQLException e){
			// 变更信息,非重要信息,当出现异常时,捕获之并且不抛出
			log.warn("写入变更信息表出现异常!", e);
		}

	}

	protected void setPs(PreparedStatement ps, AbstractMetadata metadata, String metaModelCode,
			boolean hasChildMetaModel) throws SQLException {
		// 变更类型,添加为0
		ps.setString(2, "0");
		
		// 命名空间
		String namespace = metadata.getNamespace();
		ps.setString(6, namespace == null ? genNamespace(metadata.getParentMetadata(), metadata.getId(),
				hasChildMetaModel) : namespace);
		
		// 元数据的START_TIME
		// 添加的元数据，统一采用startTime
		ps.setLong(8, startTime);	//metadata.getStartTime());
		
		// OLD_START_TIME： 删除元数据时，变更信息表的OLD_START_TIME不需要记录
		ps.setNull(10, java.sql.Types.BIGINT);
	}

	/**
	 * 获取元数据命名空间,经过缓存获取,如缓存中不存在则需自己拼装
	 * 
	 * @param parentId
	 *            父结点ID
	 * @param metadataId
	 *            当前结点ID
	 * @param hasChildMetaModel
	 *            metadataId所需模型是否有子元模型
	 * @return
	 */
	protected String genNamespace(MMMetadata parentMetadata, String metadataId, boolean hasChildMetaModel) {
		String parentId = parentMetadata.getId();
		String parentNamespace = "";
		if (parentId == null || "".equals(parentId)) { // 父结点为空,则父结点的命名空间为"/"
			parentNamespace = Constants.METADATA_PATH_SEPARATOR;
		}
		else if (namespaceCache.containsKey(parentId)) {
			if (namespaceCache.get(parentId).equals(Constants.METADATA_PATH_SEPARATOR)) {
				// 父结点肯定已经存在于缓存中,可以直接获取,此时判断父结点存储的是否为"/",此处为"/"
				parentNamespace = Constants.METADATA_PATH_SEPARATOR + parentId;
			}
			else { // 否则取出父结点缓存的value/parentId
				parentNamespace = new StringBuilder().append(namespaceCache.get(parentId)).append(
						Constants.METADATA_PATH_SEPARATOR).append(parentId).toString();

			}
		}
		else {
			parentNamespace = parentMetadata.genNamespace();
		}
		if (hasChildMetaModel) {
			// 表示metadataId底下还可能有子结点,需放入缓存中
			namespaceCache.put(metadataId, parentNamespace);
		}

		String metadataNamespace = "";
		// 判断父结点的命名空间是否为"/"
		if (parentNamespace.equals(Constants.METADATA_PATH_SEPARATOR)) {
			metadataNamespace = parentNamespace + metadataId;
		}
		else {
			metadataNamespace = new StringBuilder().append(parentNamespace).append(Constants.METADATA_PATH_SEPARATOR)
					.append(metadataId).toString();
		}
		return metadataNamespace;
	}

}
