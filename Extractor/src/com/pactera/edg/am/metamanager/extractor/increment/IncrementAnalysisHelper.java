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

package com.pactera.edg.am.metamanager.extractor.increment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Utils;

/**
 * 对新组装的元数据信息作去空格;截断;大小写转换;去重处理,待续...
 * 
 * @author user
 * @version 1.0 Date: Mar 16, 2011
 * 
 */
public class IncrementAnalysisHelper {
	private Log log = LogFactory.getLog(IncrementAnalysisHelper.class);

	private boolean toUpperCase = false, toLowerCase = false;

	public void verify(AppendMetadata aMetadata) {
		log.info("开始校验采集组装的数据...");
		Set<MMMetaModel> metaModels = aMetadata.getChildMetaModels();
		iterMetaModel(metaModels);
		// 4.去除重复
		takeOffDuplicate(aMetadata);

		log.info("数据校验完成!");

	}

	private void takeOffInDependencies(List<MMDDependency> dependencies) {
		for (Iterator<MMDDependency> depIter = dependencies.iterator(); depIter.hasNext();) {
			MMDDependency dependency = depIter.next();
			if (dependency.getOwnerMetadata().getCode() == null || dependency.getValueMetadata().getCode() == null)
				depIter.remove();
		}

	}

	private void takeOffInMetaModels(Set<MMMetaModel> metaModels) {
		for (Iterator<MMMetaModel> iter = metaModels.iterator(); iter.hasNext();) {
			MMMetaModel newMetaModel = iter.next();
			if (newMetaModel.isHasMetadata()) {
				// 处理元数据列表
				takeOffDuplicateMetadatas(newMetaModel.getMetadatas());
			}
			if (newMetaModel.isHasChildMetaModel()) {
				// 处理子元模型
				takeOffInMetaModels(newMetaModel.getChildMetaModels());
			}
		}
	}

	private void takeOffDuplicateMetadatas(List<AbstractMetadata> metadatas) {
		for (Iterator<AbstractMetadata> iter = metadatas.iterator(); iter.hasNext();) {
			AbstractMetadata metadata = iter.next();
			if (metadata.getCode() == null || metadata.getParentMetadata().getCode() == null) {
				((MMMetadata) metadata).setCode(null);
				iter.remove();
			}
		}

	}

	/**
	 * 对于重复的元数据,只取第一个,之后重复的将去除
	 * 
	 * @param metadatas
	 */
	private void takeOffDuplicate(AppendMetadata aMetadata) {
		// 4.在元数据子结点中去除
		takeOffInChildren(aMetadata.getMetadata().getChildrenMetadatas());
		// 5.需要在元模型缓存中去除
		takeOffInMetaModels(aMetadata.getChildMetaModels());
		// 6.需要在依赖关系缓存中去除
		takeOffInDependencies(aMetadata.getDDependencies());
	}

	private void takeOffInChildren(List<AbstractMetadata> metadatas) {
		Set<String> duplicate = new HashSet<String>(metadatas.size());
		for (Iterator<AbstractMetadata> iter = metadatas.iterator(); iter.hasNext();) {
			MMMetadata metadata = (MMMetadata) iter.next();
			if(metadata.getCode() == null){
				// 如存在代码为NULL,则表示有两条相同的元数据放入了列表中
				iter.remove();
				continue;
			}
			String duplicateString = metadata.getCode().concat("_").concat(metadata.getClassifierId());
			if (duplicate.contains(duplicateString)) {
				// 有重复了!那之后的子结点也不用处理了!
				metadata.setCode(null);
				iter.remove();
				continue;

			}
			else {
				duplicate.add(duplicateString);
			}
			takeOffInChildren(metadata.getChildrenMetadatas());
		}

	}

	private void iterMetaModel(Set<MMMetaModel> metaModels) {
		for (Iterator<MMMetaModel> iter = metaModels.iterator(); iter.hasNext();) {
			MMMetaModel newMetaModel = iter.next();
			if (newMetaModel.isHasMetadata()) {
				// 处理元数据列表
				dealMetadatas(newMetaModel.getMetadatas());
			}
			if (newMetaModel.isHasChildMetaModel()) {
				// 处理子元模型
				iterMetaModel(newMetaModel.getChildMetaModels());
			}
		}
	}

	private void dealMetadatas(List<AbstractMetadata> metadatas) {
		for (AbstractMetadata abstractMetadata : metadatas) {
			MMMetadata metadata = (MMMetadata) abstractMetadata;

			// 处理元数据CODE
			String code = deal(metadata.getCode(), AdapterExtractorContext.getInstance().getMaxMetadataCodeSize());
			metadata.setCode(code);
			// 对元数据名称作处理...
			String name = deal(metadata.getName(), AdapterExtractorContext.getInstance().getMaxMetadataNameSize());
			metadata.setName(name);

			// 对属性作处理
			metadata.setAttrs(dealMetadataAttrs(metadata.getAttrs()));

		}
	}

	private Map<String, String> dealMetadataAttrs(Map<String, String> attrs) {
		// 截断属性值
		Map<String, String> tgtAttrs = new HashMap<String, String>(attrs.size());
		for (Iterator<String> keyIter = attrs.keySet().iterator(); keyIter.hasNext();) {
			String key = keyIter.next();
			String value = attrs.get(key);
			if (value != null){
				// 对属性中的多个空格替换为一个空格;需先替换，再截断
				value = value.replaceAll("\\s+", " ");
				value = Utils.truncate(value, AdapterExtractorContext.getInstance().getMaxMetadataAttrSize()).trim();
				
			}
			tgtAttrs.put(key, value);
		}

		return tgtAttrs;

	}

	private String deal(String codeName, int maxSize) {
		if (codeName == null)
			return codeName;

		// 0.9替换空格(字符串内部如果有多个连续的空格，则替换为一个空格)
		codeName = codeName.replaceAll("\\s+", " ");
		// 1.截断(需先截断,再去空格)；2.去除空格
		codeName = Utils.truncate(codeName, maxSize).trim();

		// 3.大小写转换
		if (toUpperCase) {
			codeName = codeName.toUpperCase();
		}
		else if (toLowerCase) {
			codeName = codeName.toLowerCase();
		}

		return codeName;
	}

}
