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
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 添加元数据操作PreparedStatementCallback辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Aug 21, 2009
 */
public class CreateMetadataHelper extends PreparedStatementCallbackHelper {

	private Log log = LogFactory.getLog(CreateMetadataHelper.class);

	/**
	 * 存放元数据ID与命名空间的键值对,key:元数据ID,value:父结点namespace
	 */
	protected Map<String, String> namespaceCache = new HashMap<String, String>();

	protected MMMetaModel metaModel;

	public CreateMetadataHelper(int batchSize)
	{
		super(batchSize);
	}

	public void setMetaModel(MMMetaModel metaModel) {
		this.metaModel = metaModel;
	}
	
	protected Long getGlobalTime() {
		return AdapterExtractorContext.getInstance().getGlobalTime();
	}

	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException {
		// 从元模型中获取属性

		Map<String, String> mAttrs = metaModel.getMAttrs();
		boolean hasChildMetaModel = metaModel.isHasChildMetaModel();

		// 从元模型中获取属性该元数据的所有元数据
		List<AbstractMetadata> metadatas = metaModel.getMetadatas();
		int size = metadatas.size();
		String code = "";
		String metaModelCode = "";
		MMMetadata parentMetadata = null;
		String logMsg = "";
		try {
			for (int i = 0; i < size; i++) {

				MMMetadata metadata = (MMMetadata) metadatas.get(i);
				if (metadata.isHasExist()) {
					// 该元数据已经存在于库表中,将不需插入
					continue;
				}

				parentMetadata = metadata.getParentMetadata();
				if (parentMetadata == null) {
					String error = new StringBuilder("元数据:").append(metadata.getCode()).append(" 的父结点为空,请检查数据是否正确!!")
							.toString();
					log.error(error);
					throw new SQLException(error);
				}
				String metadataNamespace = genNamespace(parentMetadata, metadata.getId(), hasChildMetaModel);

				// 元数据ID
				ps.setString(1, metadata.getId());
				code = metadata.getCode();
				// 元数据名称
				ps.setString(2, code);
				// 元数据显示名
				ps.setString(3, (metadata.getName() == null || metadata.getName().equals("")) ? code : metadata.getName());
				// 元模型的ID
				metaModelCode = metaModel.getCode();
				ps.setString(4, metaModelCode);

				// namespace和父结点ID
				ps.setString(5, metadataNamespace);
				ps.setString(6, parentMetadata.getId());
				// START_TIME: 系统时间
				ps.setLong(7, this.getGlobalTime());

				int index = setAttrs(ps, metadata, mAttrs);

				setPs(ps, metadata, index + 7);

				if (log.isDebugEnabled()) {
					log.debug(new StringBuilder().append(":parent_id:").append(parentMetadata.getId()).append(
							",parent_code:").append(parentMetadata.getCode()).append(",instance_code:").append(code)
							.append(",classifier_id:").append(metaModelCode).toString());
				}
				ps.addBatch();
				// 每次都清空参数
				ps.clearParameters();

				if (++super.count % super.batchSize == 0) {
					ps.executeBatch();
					ps.clearBatch();
				}
			}

			if (super.count % super.batchSize != 0) {
				ps.executeBatch();
				ps.clearBatch();

			}
		}
		catch (SQLException e) {
			logMsg = new StringBuilder().append("数据异常,请核实数据:parent_id:").append(parentMetadata.getId()).append(
					",parent_code:").append(parentMetadata.getCode()).append(",instance_code:").append(code).append(
					",classifier_id:").append(metaModelCode).append("  异常信息:").append(e.getLocalizedMessage()).toString();
			log.error(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, logMsg);
			throw e;
		}
		return null;
		// test for callback
		// throw new SQLException();
	}

	protected int setAttrs(PreparedStatement ps, MMMetadata metadata, Map<String, String> mAttrs) throws SQLException {
		Map<String, String> attrs = getMetadataAttrPrepared(metadata);

		int index = 0;
		for (Iterator<String> iter = mAttrs.keySet().iterator(); iter.hasNext(); index++) {
			String mAttrName = iter.next();
			if (attrs.containsKey(mAttrName)) {
				// 该属性有设置值,则设置它的值；长度超长则截取
				String val = attrs.get(mAttrName) == null ? "" : attrs.get(mAttrName);
				ps.setString(index + 8, val);
			}
			else {
				// 否则设置该值为NULL
				ps.setNull(index + 8, Types.VARCHAR);
			}
		}
		return index;
	}

	/**
	 * 返回元数据属性
	 * 
	 * @return map
	 */
	protected Map<String, String> getMetadataAttrPrepared(MMMetadata metadata) {
		return metadata.getAttrs();
	}

	protected void setPs(PreparedStatement ps, MMMetadata metadata, int index) throws SQLException {

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
	private String genNamespace(MMMetadata parentMetadata, String metadataId, boolean hasChildMetaModel) {
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
