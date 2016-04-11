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

package com.pactera.edg.am.metamanager.extractor.increment.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.IMetaModelDao;
import com.pactera.edg.am.metamanager.extractor.ex.MetaModelNotFoundException;
import com.pactera.edg.am.metamanager.extractor.increment.IGenMetadataService;
import com.pactera.edg.am.metamanager.extractor.increment.IIncrementAnalysisService;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 数据比较分析,获取需要删除的依赖关系,及元数据.<br>
 * 注意:需要删除的元数据,其存储结构已经不再按照原先的元模型结构,而是全部都挂在悬挂结点下
 * 
 * @author hqchen
 * @version 1.0 Date: Oct 4, 2009
 */
public class FullIncrementAnalysisServiceImpl implements IIncrementAnalysisService {

	private Log log = LogFactory.getLog(FullIncrementAnalysisServiceImpl.class);

	/**
	 * 从存储库中获取旧元数据的DAO接口
	 */
	private IGenMetadataService genMetadataService;

	/**
	 * 操作存储库中的元模型的DAO接口
	 */
	private IMetaModelDao metaModelDao;

	/**
	 * Spring注入
	 * 
	 * @param metaModelDao
	 */
	public void setMetaModelDao(IMetaModelDao metaModelDao) {
		this.metaModelDao = metaModelDao;
	}

	public IGenMetadataService getGenMetadataService() {
		return genMetadataService;
	}

	public void setGenMetadataService(IGenMetadataService genMetadataService) {
		this.genMetadataService = genMetadataService;
	}
	
	private final static String cgb_properties = "extractor/cgb_extractor.properties";
	
	private static Properties cgbPro;
	{
		InputStream in = null;
		cgbPro = new Properties();
		try {
			in = FullIncrementAnalysisServiceImpl.class.getClassLoader()
					.getResourceAsStream(cgb_properties);
			cgbPro.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 根据悬挂结点的命名空间,获取库表中的所有子孙结点元数据,并与新数据作比较,产生需要删除的元数据
	 * 
	 * @param aNamespace
	 *            悬挂结点命名空间
	 * @return 需要删除的元模型列表(需删除的元数据存储于元模型中)
	 * @throws MetaModelNotFoundException
	 */
	private Set<MMMetaModel> genDeleteMetadatas(String aNamespace) throws MetaModelNotFoundException {
		// 不包含自身
		List<MMMetadata> dbMetadatas = genMetadataService.genChildrenMetadatasId("%"+aNamespace);
		if (dbMetadatas.size() == 0) {
			// 库表中没有数据,表示没有需要删除的数据
			return Collections.emptySet();
		}

		// 新数据从缓存中获取
		Map<String, String> newMetadatas = AdapterExtractorContext.getInstance().getNewMetadatasId();

		// 对DB中的数据排序
		Collections.sort(dbMetadatas, new Comparator<MMMetadata>() {
			public int compare(MMMetadata o1, MMMetadata o2) {
				return o1.getClassifierId().compareTo(o2.getClassifierId());
			}
		});
		
		MMMetaModel deleteMetaModel = null;
		Set<MMMetaModel> deleteMetaModels = new HashSet<MMMetaModel>();
		String metaModelCode = "";
		for (MMMetadata dbMetadata : dbMetadatas) {
			String id = dbMetadata.getId();
			if (!newMetadatas.containsKey(id)) {
				//（华润）加入条件过滤，不属于文件上传方式的采集，都需要进行已下字段落地代码的删除过滤
				if(!AdapterExtractorContext.getInstance().isFileUpload()&&
						cgbPro.get("is_del_codeItem").equals("1")&&
						(dbMetadata.getClassifierId().equals("AmColumnCodeItem")
						 ||dbMetadata.getClassifierId().equals("AmCommonCodeItemMapping"))
						){
					String parId = genMetadataService.getParId(dbMetadata.getId());
					if(parId!=null&&newMetadatas.containsKey(parId))
						continue;
				}
				// 新数据中没有,但在库表中有,表示该结点需要被删除
				if (metaModelCode.equals("")) {
					// 首次
					metaModelCode = dbMetadata.getClassifierId();
					deleteMetaModel = genMetaModel(metaModelCode);
				}

				if (!metaModelCode.equals(dbMetadata.getClassifierId())) {
					// 元模型不相同
					metaModelCode = dbMetadata.getClassifierId();
					deleteMetaModels.add(deleteMetaModel);
					deleteMetaModel = genMetaModel(metaModelCode);
				}
				deleteMetaModel.addMetadata(dbMetadata);
				deleteMetaModel.setHasMetadata(true);
			}
		}

		if (deleteMetaModel != null) {
			deleteMetaModels.add(deleteMetaModel);
		}

		return deleteMetaModels;
	}

	private MMMetaModel genMetaModel(String metaModelCode) throws MetaModelNotFoundException {
		MMMetaModel metaModel = new MMMetaModel();
		metaModel.setCode(metaModelCode);

		if (AdapterExtractorContext.getInstance().getMetaModelsAttrs().containsKey(metaModelCode)) {
			// 缓存中已经存在该模型的属性
			metaModel.setMAttrs(AdapterExtractorContext.getInstance().getMetaModelsAttrs().get(metaModelCode));
		}
		else {
			// 缓存中还不存在该属性
			metaModelDao.genStorePositionByApi(metaModel);
			AdapterExtractorContext.getInstance().addMetaModelAttrs(metaModelCode, metaModel.getMAttrs());
		}
		return metaModel;
	}

	/**
	 * 根据悬挂结点的命名空间,获取库有中的所有子孙结点的依赖关系,并通过顺序比较法,获取需要删除的依赖关系
	 * 
	 * @param aNamespace
	 *            悬挂结点的命名空间
	 * @return 需要删除的元数据依赖关系列表
	 */
	private List<MMDDependency> genDeleteDependencies(String aNamespace) {
		// 不包含自身
		List<MMDDependency> dbDependencies = genMetadataService.genDbDependencies("%"+aNamespace);
		if (dbDependencies == null || dbDependencies.size() == 0) {
			// 库表中没有数据,表示没有需要删除的数据
			return Collections.emptyList();
		}

		// 新数据元数据依赖关系,从缓存中获取
		List<MMDDependency> dependencies = AdapterExtractorContext.getInstance().getNewDependencies();
		if (dependencies.size() == 0) {
			// 新依赖关系为0,表示将需要删除库表中的所有依赖关系
			return dbDependencies;
		}

		Collections.sort(dependencies);
		Collections.sort(dbDependencies);

		int newIndex = 0, dbIndex = 0, compareInt, newSize = dependencies.size(), dbSize = dbDependencies.size();
		List<MMDDependency> deleteDependencies = new ArrayList<MMDDependency>();

		while (dbIndex < dbSize && newIndex < newSize) {
			MMDDependency dbDependency = dbDependencies.get(dbIndex);
			compareInt = dbDependency.compareTo(dependencies.get(newIndex));
			if (compareInt > 0) {
				// 循环到NEW数据的下一个,比比大小
				newIndex++;
			}
			else if (compareInt < 0) {
				String fCId = dbDependency.getOwnerMetadata().getClassifierId();
				String tCId = dbDependency.getValueMetadata().getClassifierId();
				//（华润）加入条件过滤，不属于文件上传方式的采集，都需要进行已下字段落地代码的删除过滤
				if(!AdapterExtractorContext.getInstance().isFileUpload()&&
						cgbPro.get("is_del_codeItem").equals("1")&&
						(fCId.equals("AmCommonCodeItemMapping")
							||fCId.equals("AmCommonCodeItem")
							||tCId.equals("AmCommonCodeItem")
							||tCId.equals("AmCommonCodeItemMapping"))){
						continue;
				}
				
				deleteDependencies.add(dbDependency);
				dbIndex++;
			}
			else {
				// 如两者相同,则对DB的当前结点,及NEW中的当前结点都置之不理,双双跳至下一个作比较
				newIndex++;
				dbIndex++;
			}
		}

		while (newIndex < newSize) {
			// 还剩新添加的数据,此处是把旧数据放进萝框里,其它的不管!
			break;
		}
		while (dbIndex < dbSize) {
			// 如果还剩旧数据,都装进删除列表再说!
			deleteDependencies.add(dbDependencies.get(dbIndex++));
		}

		// for (dbIndex = 0, dbSize = dbDependencies.size(); dbIndex < dbSize;)
		// {
		// MMDDependency dbDependency = dbDependencies.get(dbIndex);
		//
		// if (newIndex >= newSize) {
		// // 已经到新数据的最后
		// deleteDependencies.add(dbDependency);
		// dbIndex++;
		// continue;
		// }
		//
		// for (; newIndex < newSize;) {
		// compareInt = dbDependency.compareTo(dependencies.get(newIndex));
		// if (compareInt > 0) {
		// // 循环到NEW数据的下一个,比比大小
		// newIndex++;
		// }
		// else if (compareInt < 0) {
		// deleteDependencies.add(dbDependency);
		// dbIndex++;
		// break;
		// }
		// else {
		// // 如两者相同,则对DB的当前结点,及NEW中的当前结点都置之不理,双双跳至下一个作比较
		// newIndex++;
		// dbIndex++;
		// break;
		// }
		// }
		// }

		return deleteDependencies;
	}

	/**
	 * 增量分析产生需要删除的元数据,依赖关系
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.increment.IIncrementAnalysisService#incrementAnalysis(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public Map<Operation, AppendMetadata> incrementAnalysis(AppendMetadata aMetadata) throws Throwable {
		
		// 需要添加的悬挂结点
		AppendMetadata deleteAMetadata = aMetadata.cloneAppendMetadata();

		String aNamespace = aMetadata.getMetadata().genNamespace();
		//获取新采集或导入的缓存数据
		Map<String, String> newMetadatas = AdapterExtractorContext.getInstance().getNewMetadatasId();
		//遍历
		for(Iterator<String> sys = newMetadatas.keySet().iterator(); sys
		.hasNext();){
			String id = sys.next();
			//得到该新增数据的SchemaID
			if(newMetadatas.get(id).equals("AmModule")){
				aNamespace = id;
			}
		}
		deleteAMetadata.setChildMetaModels(genDeleteMetadatas(aNamespace));
		deleteAMetadata.setDDependencies(genDeleteDependencies(aNamespace));

		Map<Operation, AppendMetadata> incrementMetadata = new HashMap<Operation, AppendMetadata>(1);
		incrementMetadata.put(Operation.DELETE, deleteAMetadata);
		return incrementMetadata;
	}

}
