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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.pactera.edg.am.metamanager.app.metadata.bs.IMetadataService;
import com.pactera.edg.am.metamanager.core.common.SpringContextHelper;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.dao.IDeconstructorDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

public class DeconstructorDaoImpl extends JdbcDaoSupport implements IDeconstructorDao {

	private static final String separator = "/";

	public void deconstrunctor() {
		deleteBizData();
	}

	private void deleteBizData() {
		super.getJdbcTemplate().execute(GenSqlUtil.getSql("DELETE_BIZDATA_NOTIN_METADATA"));
	}

	public void deleteData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas) {
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "开始删除模板文件中，需删除的数据!");
		// 缓存
		Map<String, String> pathInstanceCache = new HashMap<String, String>();
		
		int count = 0;

		for (Iterator<String> keyIter = deleteMapDatas.keySet().iterator(); keyIter.hasNext();) {
			String classifierId = keyIter.next();
			List<Map<String, String>> deleteDatas = deleteMapDatas.get(classifierId);

			for (Map<String, String> deleteData : deleteDatas) {
				// 获取待删除的元数据
				String instanceId = getInstanceId(rootId, deleteData, pathInstanceCache);
				if (instanceId != null) {
					// 存在该元数据，则表示需要删除之
					AdapterExtractorContext.getInstance().getIClassifier().deleteMetadata(instanceId,
							AdapterExtractorContext.getInstance().getUserId());
					count++;
				}

			}
		}
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "本次采集共删除"+count+"条元数据!");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "删除模板文件中的数据完成!");
	}
	
	public void deleteMappingData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas) {
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "开始删除映射模板文件中，需删除的数据!");
		// 缓存
		Map<String, String> pathInstanceCache = new HashMap<String, String>();

		for (Iterator<String> keyIter = deleteMapDatas.keySet().iterator(); keyIter.hasNext();) {
			String classifierId = keyIter.next();
			List<Map<String, String>> deleteDatas = deleteMapDatas.get(classifierId);

			for (Map<String, String> deleteData : deleteDatas) {
				// 获取待删除的元数据
				String instanceId = getInstanceId(rootId, deleteData, pathInstanceCache);
				if (instanceId != null) {
					// 存在该元数据，则表示需要删除之
					AdapterExtractorContext.getInstance().getIClassifier().deleteMappingMetadata(instanceId,
							AdapterExtractorContext.getInstance().getUserId());
				}

			}
		}

		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "删除映射模板文件中的数据完成!");
	}

	private String getInstanceId(String rootId, Map<String, String> deleteData, Map<String, String> pathInstanceCache) {
		String path = "";
		String parentId = rootId;
		try {
			for (Iterator<String> pathIter = deleteData.keySet().iterator(); pathIter.hasNext();) {
				// 元模型
				String pathKey = pathIter.next();
				// 元模型相应的CODE，即元数据
				String pathCode = deleteData.get(pathKey);
				path = path.concat(pathCode).concat(separator);
				if (pathInstanceCache.containsKey(path)) {
					parentId = pathInstanceCache.get(path);
				}
				else {
					Map<?, ?> map = super.getJdbcTemplate().queryForMap(GenSqlUtil.getSql("FIND_CHILD_METADATA"),
							new Object[] { parentId, pathKey, pathCode });

					if (map == null || map.size() == 0) {
						// 该数据已经不存在，可能填写的数据有误，或该数据已经被删除
						return null;
					}
					// 假定存在该结点的情况下,如果不存在呢？
//					System.out.println("父结点：" + parentId + ",元模型:" + pathKey + ",CODE:" + pathCode + ",元数据ID："
//							+ map.get("INSTANCE_ID"));
					parentId = (String) map.get("INSTANCE_ID");
					pathInstanceCache.put(path, parentId);
				}

			}
		}
		catch (EmptyResultDataAccessException e) {
			// 查询数据返回没有结果，认为其已经被不存在，则返回NULL
			return null;
		}
		if (parentId != rootId)
			return parentId;
		return null;
	}

}
