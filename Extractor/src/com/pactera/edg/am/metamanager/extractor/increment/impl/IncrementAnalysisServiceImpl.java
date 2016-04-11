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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleCover;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.ModifyMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.IMetaModelDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao;
import com.pactera.edg.am.metamanager.extractor.dao.ISequenceDao;
import com.pactera.edg.am.metamanager.extractor.ex.CompositionNotFoundException;
import com.pactera.edg.am.metamanager.extractor.ex.MDependencyNotFoundException;
import com.pactera.edg.am.metamanager.extractor.ex.MetaModelNotFoundException;
import com.pactera.edg.am.metamanager.extractor.increment.IGenMetadataService;
import com.pactera.edg.am.metamanager.extractor.increment.IIncrementAnalysisService;
import com.pactera.edg.am.metamanager.extractor.increment.IncrementAnalysisHelper;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 实现增量分析功能
 * 
 * @author hqchen
 * @version 1.0 Date: Sep 29, 2009
 * 
 */
public class IncrementAnalysisServiceImpl implements IIncrementAnalysisService {

	private Log log = LogFactory.getLog(IncrementAnalysisServiceImpl.class);
 
	/**
	 * 从存储库中获取旧元数据的DAO接口
	 */
	private IGenMetadataService genMetadataService;

	/**
	 * SequenceDao
	 */
	private ISequenceDao sequenceDao;

	/**
	 * 操作存储库中的元模型的DAO接口
	 */
	private IMetaModelDao metaModelDao;
	
	/**
	 * 操作存储库中的元数据的DAO接口
	 */
	private IMetadataDao metaDataDao;

	/**
	 * 库表中不存在的元模型属性
	 */
	private Map<String, Set<String>> errorAttrs = new HashMap<String, Set<String>>();

	/**
	 * 库表中不存在的元模型的组合关系
	 */
	private Set<String> errorCompositions = new HashSet<String>(0);

	/**
	 * 全局的悬挂结点,方便在该类的任意地方随意使用
	 */
	private AppendMetadata aMetadata;

	/**
	 * Spring注入
	 * 
	 * @param sequenceDao
	 */
	public void setSequenceDao(ISequenceDao sequenceDao) {
		this.sequenceDao = sequenceDao;
	}

	public void setGenMetadataService(IGenMetadataService genMetadataService) {
		this.genMetadataService = genMetadataService;
	}

	public IMetadataDao getMetaDataDao() {
		return metaDataDao;
	}

	public void setMetaDataDao(IMetadataDao metaDataDao) {
		this.metaDataDao = metaDataDao;
	}

	/**
	 * 增量分析需要添加,修改的元数据及依赖关系
	 * 
	 * @param aMetadata
	 *            新悬挂结点以下的树
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.increment.IIncrementAnalysisService#incrementAnalysis(java.lang.ref.SoftReference)
	 */
	public Map<Operation, AppendMetadata> incrementAnalysis(AppendMetadata aMetadata) throws Throwable {

		this.aMetadata = aMetadata;
		beforeIncrementAnalysis();

		// 需要添加的悬挂结点
		AppendMetadata createAMetadata = aMetadata.cloneAppendMetadata();
		// 需要修改的悬挂结点
		AppendMetadata modifyAMetadata = aMetadata.cloneAppendMetadata();

		try {
			String dCode = aMetadata.getMetadata().getCode();
			String mCode = aMetadata.getMetaModel().getCode();
			// --------------------处理元数据增量分析------------------------
			// 悬挂结点的ID
			String logMsg = new StringBuilder("开始对悬挂结点:").append(dCode).append(",模型:").append(mCode).append(
					" 的所有子孙结点作增量分析...").toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

			List<AbstractMetadata> metadatas = getAMetadatas(aMetadata);
			// 悬挂结点的子元模型
			Set<MMMetaModel> newMetaModels = aMetadata.getChildMetaModels();
			Set<MMMetaModel> createMetaModels = new HashSet<MMMetaModel>(newMetaModels.size());
			Set<MMMetaModel> modifyMetaModels = new HashSet<MMMetaModel>(newMetaModels.size());

			for (Iterator<MMMetaModel> iter = newMetaModels.iterator(); iter.hasNext();) {
				MMMetaModel newMetaModel = iter.next();
					if (newMetaModel.isHasMetadata()) {
						// 有该元模型的元数据
						// 比较产生需要添加,修改的元数据列表
						MMMetaModel createMetaModel = newMetaModel.cloneOnlyMetaModel();
						MMMetaModel modifyMetaModel = newMetaModel.cloneOnlyMetaModel();
						compareRootMetaModel(createMetaModel, modifyMetaModel, metadatas, newMetaModel);
						/**
						 * @author wengn
						 * @date 2011-11-9
						 * @desc 导标准时不能改变字典的数据
						 */
						if(newMetaModels.size()!=1 && "AmSystem".equals(newMetaModel.getCode())){
							continue;
						}else{
							createMetaModels.add(createMetaModel);
							modifyMetaModels.add(modifyMetaModel);
						}
				}
			}

			createAMetadata.setChildMetaModels(createMetaModels);
			modifyAMetadata.setChildMetaModels(modifyMetaModels);

			Map<Operation, AppendMetadata> incrementMetadata = new HashMap<Operation, AppendMetadata>(2);
			incrementMetadata.put(Operation.CREATE, createAMetadata);
			incrementMetadata.put(Operation.MODIFY, modifyAMetadata);

			log.info("元数据的增量分析完成!");
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "元数据的增量分析完成!");
			// ------------------处理依赖关系增量分析----------------------------
			logMsg = new StringBuilder("开始对悬挂结点:").append(dCode).append(",模型:").append(mCode).append(
					" 的所有子孙结点的依赖关系作增量分析...").toString();
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

			List<MMDDependency> dependencies = aMetadata.getDDependencies();
			if (dependencies != null) {
				// 可能为空
				createAMetadata.setDDependencies(genCreateDependencies(dependencies,incrementMetadata.get(Operation.CREATE)));
			}
			log.info("元数据依赖关系的增量分析完成!");
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "元数据依赖关系的增量分析完成!");

			printWarn();
			return incrementMetadata;
		}
		catch (Throwable t) {
			throw t;
		}
		finally {
			// 最后清空缓存
			errorAttrs.clear();
			errorCompositions.clear();
		}
	}

	private void beforeIncrementAnalysis() {
		IncrementAnalysisHelper helper = new IncrementAnalysisHelper();
		helper.verify(aMetadata);

	}

	/**
	 * 打印出错信息,当存在组合关系模型缺失时,统计完将抛出异常
	 * 
	 * @throws CompositionNotFoundException
	 */
	private void printWarn() throws CompositionNotFoundException {
		String logMsg = null;
		if (errorAttrs.size() > 0) {
			logMsg = "元数据库中的元模型不存在的属性,请检查库表中的关系是否缺失,或程序是否正确!如下:\n" + errorAttrs.toString();
			log.warn(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		}
		if (errorCompositions.size() > 0) {
			logMsg = "元数据库中的元模型不存在如下组合关系,请检查库表中的关系是否缺失," +
					"或数据源的悬挂点元数据的类型是否正确!如下:\n" + errorCompositions.toString();
			log.error(logMsg);
			// 如组合关系缺失,则认为错误比较严重,将抛出异常!
			throw new CompositionNotFoundException(logMsg);
		}

	}

	/**
	 * 根据悬挂结点命名空间,从库表中获取所有子孙结点的依赖关系,并经过与新数据作顺序比较法,获取需要添加的元数据依赖关系
	 * 
	 * @param dependencies
	 *            新的元数据依赖关系
	 * @param aNamespace
	 *            悬挂结点命名空间
	 * @return 需要添加的元数据依赖关系
	 * @throws MDependencyNotFoundException
	 */
	private List<MMDDependency> genCreateDependencies(List<MMDDependency> dependencies,AppendMetadata createAMetadata)
			throws MDependencyNotFoundException {

		String aNamespace = aMetadata.getMetadata().genNamespace();
		// 获取数据库中metadataId结点的所有子孙结点之间的依赖关系
		List<MMDDependency> dbDependencies;

		String aMetaModelCode = aMetadata.getMetaModel().getCode();
		if ("ETLActivity".equals(aMetaModelCode) || "Catalog".equals(aMetaModelCode)
				|| "Pc86PowerCenter".equals(aMetaModelCode) || "AmRepository".equals(aMetaModelCode)
				|| "MappingFolder".equals(aMetaModelCode)) { // ||
			// "AmDataStandard".equals(aMetaModelCode))
			// {
			// 悬挂结点的元模型为ETEL工程,或为Catalog

			dbDependencies = genSubCreateDependencies();
		}
		else {
			// 获取子孙结点的依赖关系(不包括自身)
			dbDependencies = genMetadataService.genDbDependencies(aNamespace.concat("/"));
		}

		return comareDependencies(dependencies, dbDependencies,createAMetadata);
	}

	private List<MMDDependency> genSubCreateDependencies() {
		List<MMDDependency> dbDependencies = new ArrayList<MMDDependency>();
		Set<MMMetaModel> metaModels = aMetadata.getChildMetaModels();

		for (MMMetaModel metaModel : metaModels) {
			if (metaModel.isHasMetadata()) {
				List<AbstractMetadata> metadatas = metaModel.getMetadatas();

				// 从库表中获取该类元模型的元数据
				List<AbstractMetadata> dbMetadatas = genMetadataService.genChildMetadatas(getAMetadatas(aMetadata),
						metaModel);

				if (metadatas.size() > Constants.MAX_METADATA_SIZE) {
					// 该层的元数据记录数过大,考虑采用IN的方式获取子孙结点的依赖关系
					Set<String> metadataNamespaces = new HashSet<String>(metadatas.size());
					for (AbstractMetadata metadata : metadatas) {
						// int compareInt =
						// Collections.binarySearch(dbMetadatas, metadata,
						// new Comparator<AbstractMetadata>() {
						//
						// public int compare(AbstractMetadata o1,
						// AbstractMetadata o2) {
						// return o1.getCode().compareTo(o2.getCode());
						// }
						// });
						// if (compareInt >= 0) {
						// metadataNamespaces.add(dbMetadatas.get(compareInt).getNamespace());
						// }

						String mdCode = metadata.getCode();
						for (AbstractMetadata dbMetadata : dbMetadatas) {
							if (dbMetadata.getCode().equals(mdCode)) {
								// 判断到需要入库的数据,在库表中已经存在,此时有必要从库表中获取数据,然后与需要入库的数据来比试一番
								metadataNamespaces.add(((MMMetadata) dbMetadata).genNamespace());
								break;
							}
						}
					}
					dbDependencies.addAll(genMetadataService.genDbDependencies(metadataNamespaces));

				}
				else {
					for (AbstractMetadata metadata : metadatas) {
						String mdCode = metadata.getCode();
						// 此处处理欠缺考究,当该层结点过多时,性能会差很多!!此外有待完善
						for (AbstractMetadata dbMetadata : dbMetadatas) {
							if (dbMetadata.getCode().equals(mdCode)) {
								// 判断到需要入库的数据,在库表中已经存在,此时有必要从库表中获取数据,然后与需要入库的数据来比试一番
								String namespace = ((MMMetadata) dbMetadata).genNamespace();
								// 获取子孙结点的依赖关系(包含自身)
								dbDependencies.addAll(genMetadataService.genDbDependencies(namespace));
								break;
							}
						}
					}
				}
			}
		}

		return dbDependencies;
	}

	/**
	 * 用批量数据比较法产生需要添加的依赖关系
	 * 
	 * @param dependencies
	 *            新元数据依赖关系
	 * @param dbDependencies
	 *            库表中的元数据依赖关系
	 * @return 需要添加的元数据依赖关系
	 * @throws MDependencyNotFoundException
	 */
	private List<MMDDependency> comareDependencies(List<MMDDependency> dependencies, List<MMDDependency> dbDependencies,AppendMetadata createAMetadata)
			throws MDependencyNotFoundException {
		//缓存无效的规则ID
		List<String> invalidRuleIds = new ArrayList<String>();
		//需要添加的元数据依赖关系
		List<MMDDependency> createDependencies = new ArrayList<MMDDependency>();

		// 赋于依赖关系code
		genRelationCode(dependencies);

		if (dbDependencies == null || dbDependencies.size() == 0) {
			// 库表中不存在依赖关系,所有新数据都将进库
			//AdapterExtractorContext.getInstance().setNewDependency(dependencies);
			//return dependencies;
			
			for(MMDDependency newDependency:dependencies){
				/**
				 * @author wengn
				 * @date 2011-12-9
				 * @desc 创建依赖关系时要先要根据元数据库表中落地字段是否真实存在来判断关系是否需要建立
				 */
				getInvalidIds(invalidRuleIds, createDependencies,newDependency);
				
				/**
				 * @author wengn
				 * @date 2011-12-9
				 * @desc 过滤依赖关系和元模型列表避免创建无效的依赖关系和元数据
				*/ 
				if(invalidRuleIds.size()!=0 && createDependencies.size()!=0){
					createDependencyFilter(createAMetadata, invalidRuleIds,createDependencies);
				}
			}
			return createDependencies;
		}

		// 对库表中的依赖关系排序
		Collections.sort(dbDependencies);
		Collections.sort(dependencies);

		int dbIndex = 0, newIndex = 0, compareInt, newSize = dependencies.size(), dbSize = dbDependencies.size();

		while (newIndex < newSize && dbIndex < dbSize) {
			MMDDependency newDependency = dependencies.get(newIndex);

			compareInt = newDependency.compareTo(dbDependencies.get(dbIndex));
			if (compareInt > 0) {
				// 循环到DB的下一个,比比大小
				dbIndex++;
			}
			else if (compareInt < 0) {
				/**
				 * @author wengn
				 * @date 2011-12-1
				 * @desc 创建依赖关系时要先要根据元数据库表中落地字段是否真实存在来判断关系是否需要建立
				 */
				getInvalidIds(invalidRuleIds, createDependencies,newDependency);
				newIndex++;
			}
			else {
				// 如两者相同,则对DB的当前结点,及NEW中的当前结点都置之不理,双双跳至下一个作比较
				dbIndex++;
				newIndex++;
				// 缓存没有变动的依赖关系
				AdapterExtractorContext.getInstance().addDependency(newDependency);
				
			}
		}
		while (newIndex < newSize) {
			// 还剩新添加的数据,都装进添加列表再说!
			MMDDependency dependency = dependencies.get(newIndex++);
			// 已经到DB的最后
			createDependencies.add(dependency);
			// 缓存需要添加的依赖关系
			AdapterExtractorContext.getInstance().addDependency(dependency);
		}
		while (dbIndex < dbSize) {
			// 如果还剩旧数据,则不管它
			break;
		}
		/**
		 * @author wengn
		 * @date 2011-12-6
		 * @desc 过滤依赖关系和元模型列表避免创建无效的依赖关系和元数据
		*/ 
		if(invalidRuleIds.size()!=0 && createDependencies.size()!=0){
			createDependencyFilter(createAMetadata, invalidRuleIds,createDependencies);
		}
		return createDependencies;
	}

	/**
	 * 过滤与无效的规则ID相关联的依赖关系和元数据
	 * @author wengn
	 * @date 2011-12-6
	 * @param createAMetadata 需要创建的元模型
	 * @param invalidRuleIds 无效的规则ID列表
	 * @param createDependencies 需要创建的依赖关系列表
	 */
	private void createDependencyFilter(AppendMetadata createAMetadata,
			List<String> invalidRuleIds, List<MMDDependency> createDependencies) {
		List<MMDDependency> tempDependencies = new ArrayList<MMDDependency>();//用来做比较的临时容器
		for(MMDDependency dependency:createDependencies){
			tempDependencies.add(dependency);
		}
		for(int i=0;i<tempDependencies.size();i++){
			MMDDependency dependency = tempDependencies.get(i);
			for(int j=0;j<invalidRuleIds.size();j++){
				String invalidRuleId = (String)invalidRuleIds.get(j);
				if(dependency.getOwnerMetadata().getCode().indexOf(invalidRuleId)!=-1 || dependency.getValueMetadata().getCode().indexOf(invalidRuleId)!=-1){
					//找到和无效规则ID匹配的依赖关系则从依赖关系列表中移除
					createDependencies.remove(dependency);
				}
			}
		}
		
		Set<MMMetaModel> metaModels = createAMetadata.getChildMetaModels();
		//依赖关系过滤完成之后还要过滤需要添加的元模型列表,避免生成无效的元数据
		createMetaModelsFilter(invalidRuleIds, metaModels);
		//将过滤后的元模型列表重新设置回去
		createAMetadata.setChildMetaModels(metaModels);
	}

	/**
	 * 获取无效的规则ID列表
	 * @author wengn
	 * @date 2011-12-6
	 * @param invalidRuleIds 无效规则ID的列表
	 * @param createDependencies 需要生成的依赖关系列表
	 * @param newDependency 等待生成的依赖关系
	 */
	private void getInvalidIds(List<String> invalidRuleIds,
			List<MMDDependency> createDependencies, MMDDependency newDependency) {
		boolean flag = true;
		if(newDependency.getCode().indexOf("-AmColumn-")!=-1 || newDependency.getCode().indexOf("-AmColumnCodeItem-")!=-1){//涉及落地字段或落地字段代码项的元模型依赖关系
			if(!"Dep-AmColumn-AmCommonRealCode".equals(newDependency.getCode())){//导字典数据时公共落地代码依赖不需要进行判断
				//查询库表中是否真实存在
				flag = metaDataDao.existMetadata(newDependency.getOwnerMetadata().getParentMetadata().getId(), newDependency.getOwnerMetadata());
			}
		}
		if(flag){//存在则直接添加即可
			createDependencies.add(newDependency);
			// 缓存需要添加的依赖关系
			AdapterExtractorContext.getInstance().addDependency(newDependency);
		}else{//不存在的话不仅该条依赖关系不能添加,而且涉及到该依赖关系的关联依赖关系以及关联元数据也不能添加,这里先缓存起来,最后再做一次过滤
			String tempRuleId = newDependency.getValueMetadata().getCode();
			String invalidRuleId = "";
			if(tempRuleId.indexOf("-")!=-1){
				invalidRuleId = tempRuleId.substring(0,tempRuleId.indexOf("-"));
			}else{
				invalidRuleId = tempRuleId;
			}
			invalidRuleIds.add(invalidRuleId);
			//记录日志
			log.info("映射规则: "+invalidRuleId+" 添加失败,元数据库表中不存在对应的元数据!");
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.WARN,"映射规则: "+invalidRuleId+" 添加失败,元数据库表中不存在对应的元数据!");
		}
	}

	/**
	 * 过滤元模型列表避免生成无效的元数据信息 
	 * @author wengn
	 * @date 2011-11-20
	 * @param invalidRuleIds 无效的规则ID列表
	 * @param metaModels 需要生成的元模型列表
	 */
	private void createMetaModelsFilter(List<String> invalidRuleIds,
			Set<MMMetaModel> metaModels) {
		for (MMMetaModel metaModel : metaModels) {
			if (metaModel.isHasMetadata()) {
				List<AbstractMetadata> list = metaModel.getMetadatas();
				List<AbstractMetadata> tempList = new ArrayList<AbstractMetadata>();// 用来比较的临时容器
				for (AbstractMetadata am : list) {
					tempList.add(am);
				}
				for (AbstractMetadata am : tempList) {
					for (int j = 0; j < invalidRuleIds.size(); j++) {
						String invalidRuleId = invalidRuleIds.get(j);
						if (am.getCode().indexOf(invalidRuleId) != -1) {
							// 找到和无效依赖关系匹配的元数据则移除
							list.remove(am);
						}
					}
				}
			}
			
			//递归过滤子元模型
			if(metaModel.isHasChildMetaModel()) {
				createMetaModelsFilter(invalidRuleIds,metaModel.getChildMetaModels());
			}
		}
	}

	/**
	 * 获取元数据依赖关系的CODE:先到缓存中查找,如不存在,再到库表中查找,如库表缺失依赖关系,则先缓存起来,完了再统一统计异常, 并抛出异常,
	 * 
	 * @param dependencies
	 * @throws MDependencyNotFoundException
	 */
	private void genRelationCode(List<MMDDependency> dependencies) throws MDependencyNotFoundException {
		Map<String, String> relationCodeCache = AdapterExtractorContext.getInstance().getRelationCodeCache();

		Set<String> errorCodes = new HashSet<String>();
		for (MMDDependency dependency : dependencies) {
			// 得到依赖关系的HASHCODE
			// int hashCode = dependency.hashCode();
			String relationMetaModel = new StringBuilder(dependency.getOwnerMetadata().getClassifierId()).append("_").append(dependency.getOwnerRole()).append("_")
					.append(dependency.getValueMetadata().getClassifierId()).append("_").append(dependency.getValueRole()).toString();

			if (relationCodeCache.containsKey(relationMetaModel)) {
				// 缓存中是否存在该依赖关系的HASHCODE
				String code = relationCodeCache.get(relationMetaModel);
				if (code != null) {
					dependency.setCode(code);
				}
			}
			else {
				String code = metaModelDao.genRelationCode(dependency);
				if (code == null) {
					errorCodes.add(new StringBuilder("依赖对象的元模型:").append(
							dependency.getOwnerMetadata().getClassifierId()).append(",角色:").append(
							dependency.getOwnerRole()).append(",被依赖对象的元模型:").append(
							dependency.getValueMetadata().getClassifierId()).append(",角色:").append(
							dependency.getValueRole()).toString());
				}
				else {
					dependency.setCode(code);
					// 把查找过的缓存起来
					relationCodeCache.put(relationMetaModel, dependency.getCode());
				}
			}
		}
		if (errorCodes.size() > 0) {
			String logMsg = "元数据库表中不存在如下依赖关系:\n" + errorCodes.toString();
			log.error(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, logMsg);

			throw new MDependencyNotFoundException(errorCodes.toString());
		}

	}

	private List<AbstractMetadata> getAMetadatas(AppendMetadata aMetadata) {
		List<AbstractMetadata> dsMetadatas = new ArrayList<AbstractMetadata>(1);
		dsMetadatas.add(aMetadata.getMetadata());
		return dsMetadatas;
	}

	/**
	 * 比较子孙元模型,获取需要修改的,添加的元数据
	 * 
	 * @param createMetaModel
	 *            需要添加的元模型
	 * @param modifyMetaModel
	 *            需要修改的元模型
	 * @param parentMetadatas
	 *            父结点列表,指当前模型的父模型的所有元数据结点
	 * @param newMetaModel
	 *            新的元数据
	 * @throws MetaModelNotFoundException
	 */
	private void compareRootMetaModel(MMMetaModel createMetaModel, MMMetaModel modifyMetaModel,
			List<AbstractMetadata> parentMetadatas, MMMetaModel newMetaModel) throws MetaModelNotFoundException {

		String aMetaModelCode = aMetadata.getMetaModel().getCode();
		if ("ETLActivity".equals(aMetaModelCode) || "Catalog".equals(aMetaModelCode)
				|| "Pc86PowerCenter".equals(aMetaModelCode) || "AmRepository".equals(aMetaModelCode)
				|| "MappingFolder".equals(aMetaModelCode) || "AmDataStandard".equals(aMetaModelCode)) {
			// 悬挂结点的元模型为ETEL工程,或为Catalog

			// 获取元模型的属性,属性所在位置,及组合关系CODE
			genMetaModelInfo(createMetaModel, modifyMetaModel);

			// 从库表中获取数据
			List<AbstractMetadata> dbMetadatas = genMetadataService.genChildMetadatas(parentMetadatas, createMetaModel);

			List<AbstractMetadata> realDbMetadatas = new ArrayList<AbstractMetadata>();
			List<AbstractMetadata> newMetadatas = newMetaModel.getMetadatas();
			for (AbstractMetadata newMetadata : newMetadatas) {
				String newMetadataCode = newMetadata.getCode();
				for (AbstractMetadata dbMetadata : dbMetadatas) {
					if (dbMetadata.getCode().equals(newMetadataCode)) {
						realDbMetadatas.add(dbMetadata);
					}
				}
			}
			// 此时已经可以将父结点列表置为空
			// parentMetadatas = null;
			// 对数据的比较处理
			compareMetadatas(createMetaModel, modifyMetaModel, newMetaModel, realDbMetadatas);

			if (newMetaModel.isHasChildMetaModel()) {
				// 新元模型还有子元模型
				createMetaModel.setHasChildMetaModel(true);
				modifyMetaModel.setHasChildMetaModel(true);
				genChildMetaModels(createMetaModel, modifyMetaModel, realDbMetadatas, newMetaModel);
			}
		}
		else {
			compareMetaModel(createMetaModel, modifyMetaModel, parentMetadatas, newMetaModel);
		}
	}

	/**
	 * 比较子孙元模型,获取需要修改的,添加的元数据
	 * 
	 * @param createMetaModel
	 *            需要添加的元模型
	 * @param modifyMetaModel
	 *            需要修改的元模型
	 * @param parentMetadatas
	 *            父结点列表,指当前模型的父模型的所有元数据结点
	 * @param newMetaModel
	 *            新的元数据
	 * @throws MetaModelNotFoundException
	 */
	private void compareMetaModel(MMMetaModel createMetaModel, MMMetaModel modifyMetaModel,
			List<AbstractMetadata> parentMetadatas, MMMetaModel newMetaModel) throws MetaModelNotFoundException {

		// 获取元模型的属性,属性所在位置,及组合关系CODE
		genMetaModelInfo(createMetaModel, modifyMetaModel);

		// 从库表中获取数据
		List<AbstractMetadata> dbMetadatas = genMetadataService.genChildMetadatas(parentMetadatas, createMetaModel);
		// 此时已经可以将父结点列表置为空
		// parentMetadatas = null;
		// 对数据的比较处理
		compareMetadatas(createMetaModel, modifyMetaModel, newMetaModel, dbMetadatas);

		if (newMetaModel.isHasChildMetaModel()) {
			// 新元模型还有子元模型
			createMetaModel.setHasChildMetaModel(true);
			modifyMetaModel.setHasChildMetaModel(true);
			genChildMetaModels(createMetaModel, modifyMetaModel, dbMetadatas, newMetaModel);
		}

	}

	/**
	 * 获取元模型的属性信息,组合关系信息,并缓存起来,供多次使用,减少数据库的IO操作
	 * 
	 * @param createMetaModel
	 *            需要添加的元模型
	 * @param modifyMetaModel
	 *            需要修改的元模型
	 * @throws MetaModelNotFoundException
	 */
	private void genMetaModelInfo(MMMetaModel createMetaModel, MMMetaModel modifyMetaModel)
			throws MetaModelNotFoundException {
		String parentMCode = createMetaModel.getParentMetaModel().getCode();
		String code = createMetaModel.getCode();
		String codeKey = new StringBuilder(parentMCode).append("_").append(code).toString();

		if (AdapterExtractorContext.getInstance().getMetaModelsAttrs().containsKey(codeKey)) {
			// 缓存中已经存在
			createMetaModel.setMAttrs(AdapterExtractorContext.getInstance().getMetaModelsAttrs().get(codeKey));
			createMetaModel.setCompedRelationCode(AdapterExtractorContext.getInstance().getMetaModelsComp()
					.get(codeKey));

			modifyMetaModel.setMAttrs(AdapterExtractorContext.getInstance().getMetaModelsAttrs().get(codeKey));
			modifyMetaModel.setCompedRelationCode(AdapterExtractorContext.getInstance().getMetaModelsComp()
					.get(codeKey));
		}
		else {
			try {
				metaModelDao.genStorePositionByApi(createMetaModel);
				metaModelDao.setCompedRelation(createMetaModel.getParentMetaModel(), createMetaModel);
			}
			catch (CompositionNotFoundException c) {
				errorCompositions.add(new StringBuilder().append(parentMCode).append("-->").append(code).toString());
			}

			modifyMetaModel.setMAttrs(createMetaModel.getMAttrs());
			modifyMetaModel.setCompedRelationCode(createMetaModel.getCompedRelationCode());

			AdapterExtractorContext.getInstance().addMetaModelAttrs(codeKey, createMetaModel.getMAttrs());
			AdapterExtractorContext.getInstance().addMetaModelComp(codeKey, createMetaModel.getCompedRelationCode());
		}

	}

	/**
	 * 递归比较子孙元模型的元数据,从而产生需要添加,修改的元数据
	 * 
	 * @param createMetaModel
	 *            需要添加的元模型
	 * @param modifyMetaModel
	 *            需要修改的元模型
	 * @param parentMetadatas
	 *            属于当前模型的父元模型的元数据列表
	 * @param newMetaModel
	 * @throws MetaModelNotFoundException
	 */
	private void genChildMetaModels(MMMetaModel createMetaModel, MMMetaModel modifyMetaModel,
			List<AbstractMetadata> parentMetadatas, MMMetaModel newMetaModel) throws MetaModelNotFoundException {
		Set<MMMetaModel> childNewMetaModels = newMetaModel.getChildMetaModels();

		Set<MMMetaModel> childCreateMetaModels = new HashSet<MMMetaModel>(childNewMetaModels.size());
		Set<MMMetaModel> childModifyMetaModels = new HashSet<MMMetaModel>(childNewMetaModels.size());

		for (Iterator<MMMetaModel> iter = childNewMetaModels.iterator(); iter.hasNext();) {
			MMMetaModel childNewMetaModel = iter.next();
			if (childNewMetaModel.isHasMetadata()) {
				// 有该元模型的元数据
				// 比较产生需要添加,修改的元数据列表
				MMMetaModel childCreateMetaModel = childNewMetaModel.cloneOnlyMetaModel();
				MMMetaModel childModifyMetaModel = childNewMetaModel.cloneOnlyMetaModel();
				compareMetaModel(childCreateMetaModel, childModifyMetaModel, parentMetadatas, childNewMetaModel);
				childCreateMetaModels.add(childCreateMetaModel);
				childModifyMetaModels.add(childModifyMetaModel);
			}
		}
		createMetaModel.setChildMetaModels(childCreateMetaModels);
		modifyMetaModel.setChildMetaModels(childModifyMetaModels);

	}

	/**
	 * 批量顺序比较新的元数据列表与库表中的元数据列表,产生本模型的需要添加,修改的元数据列表
	 * 
	 * @param createMetaModel
	 * @param modifyMetaModel
	 * @param newMetadatas
	 * @param dbMetadatas
	 */
	private void compareMetadatas(MMMetaModel createMetaModel, MMMetaModel modifyMetaModel, MMMetaModel newMetaModel,
			List<AbstractMetadata> dbMetadatas) {

		List<AbstractMetadata> newMetadatas = newMetaModel.getMetadatas();
		int dbSize = dbMetadatas.size(), newSize = newMetadatas.size();

		// 数据已经排过序
		if (dbSize == 0) {
			// 旧数据为空,则所有新数据都为需要添加的元数据
			genCreateMetaModel(createMetaModel, newMetadatas);
			return;
		}
		if (newSize == 0) {
			// 新元数据为空
			return;
		}

		// 对数据进行排序
		Collections.sort(newMetadatas, new MetadataComparator());
		Collections.sort(dbMetadatas, new MetadataComparator());

		int dbIndex = 0, newIndex = 0, compareInt;
		// 元模型的属性
		Map<String, String> mAttrs = createMetaModel.getMAttrs();
		
		List<AbstractMetadata> createMetadatas = new ArrayList<AbstractMetadata>(newSize >> 2);
		List<AbstractMetadata> modifyMetadatas = new ArrayList<AbstractMetadata>(newSize >> 2);

		while (newIndex < newSize && dbIndex < dbSize) {
			MMMetadata newMetadata = (MMMetadata) newMetadatas.get(newIndex);
			// -------应该对属性作校验----------
			checkMetaModel(newMetadata, mAttrs);
			MMMetadata dbMetadata = (MMMetadata) dbMetadatas.get(dbIndex);
			newMetadata.setStartTime(dbMetadata.getStartTime());
			// 新旧数据作比较
			compareInt = newMetadata.compareTo2(dbMetadata);
			if (compareInt > 0) {
				// 循环到db数据的下一个,比比大小
				dbIndex++;
			}
			else if (compareInt < 0) {
				createMetadatas.add(newMetadata);
				newIndex++;
			}
			else {
				// 如两者相同,则对DB的当前结点,及NEW中的当前结点都置之不理
				// 为该结点的子孙结点作比较时所用
				
				dealSameMetadata(modifyMetadatas, newMetaModel, newMetadata, dbMetadata);
				// 双双跳至下一个作比较
				newIndex++;
				dbIndex++;
			}

		}
		while (newIndex < newSize) {
			// 还剩新添加的数据,都装进添加列表再说!
			MMMetadata newMetadata = (MMMetadata) newMetadatas.get(newIndex++);
			// -------应该对属性作校验----------
			checkMetaModel(newMetadata, mAttrs);
			createMetadatas.add(newMetadata);
		}

		while (dbIndex < dbSize) {
			// 如果还剩旧数据,则不管它
			break;
		}

		if (createMetadatas.size() > 0) {
			// 为需要增加的元数据赋于ID
			genCreateMetaModel(createMetaModel, createMetadatas);
		}
		if (modifyMetadatas.size() > 0) {
			modifyMetaModel.setMetadatas(modifyMetadatas);
			modifyMetaModel.setHasMetadata(true);
		}
	}

	private void dealSameMetadata(List<AbstractMetadata> modifyMetadatas, MMMetaModel newMetaModel,
			MMMetadata newMetadata, MMMetadata dbMetadata) {
		newMetadata.setId(dbMetadata.getId());
		//获取已存储的sheet字段cover配置信息
		Map<String,TemplateTitleCover> tmplTitle = AdapterExtractorContext.getInstance().getIsCover();
		// 把元数据名称作比较
		String newName = newMetadata.getName() == null ? newMetadata.getCode() : newMetadata.getName();
		boolean isSameName = newName.equals(dbMetadata.getName());
		//		//----add by xks  定制数据库直采数据字典类型增量策略------------
		if(AdapterExtractorContext.getInstance().getIsDBExtract()
				&&tmplTitle!=null){
			String dbClsId = dbMetadata.getClassifierId();
			TemplateTitleCover title = tmplTitle.get(dbClsId);
			//在配置的cover不为空的情况下进行比对设置
			if(title!=null){
				//当当前覆盖状态为1的情况下
				if(title.getIsNameCover()!=null&&title.getIsNameCover().equals("1")){
					isSameName=true;
				}
			}
		}
		
		
		//----------adn by xks----------------
		
		if(newMetadata.getClassifierId().equals("Table"))
		{
			newMetadata.delAttr("nextExtent");
			newMetadata.addAttr("nextExtent", dbMetadata.getAttr("nextExtent"));
		}
		
		// 比较两元数据的属性
		ModifyMetadata mMetadata = null;
		mMetadata = newMetadata.genDiffAttrs(newMetaModel, dbMetadata);

		if (mMetadata != null) {
			// 表示元数据有属性发生变化,将不存在于库表中的属性去除
			mMetadata = cleanNotExistAttr(mMetadata, isSameName);
			if (mMetadata != null) {
				// 添加父结点
				mMetadata.setParentMetadata(dbMetadata.getParentMetadata());
				mMetadata.setCode(newMetadata.getCode());
				modifyMetadatas.add(mMetadata);
				// 缓存需要修改的元数据ID
				AdapterExtractorContext.getInstance().addNewMetadataId(mMetadata.getId(), mMetadata.getClassifierId());
			}
			else {
				// 缓存没有发生变化的元数据ID
				AdapterExtractorContext.getInstance().addNewMetadataId(dbMetadata.getId(),
						newMetadata.getClassifierId());
			}
		}
		else if (isSameName) {
			// 缓存没有发生变化的元数据ID
			AdapterExtractorContext.getInstance().addNewMetadataId(dbMetadata.getId(), newMetadata.getClassifierId());
		}
		else {
			// 元数据名称发生变化,元数据属性没有发生变化
			if (!AdapterExtractorContext.getInstance().getIsDBExtract()&&newMetaModel.isHaveChangelessAttr()) {
				// 有该属性，则名称也属于不变化的范畴
				AdapterExtractorContext.getInstance().addNewMetadataId(dbMetadata.getId(),
						newMetadata.getClassifierId());
			}
			else {
				ModifyMetadata modifyMetadata = new ModifyMetadata();
				// 添加元数据的父结点
				modifyMetadata.setParentMetadata(dbMetadata.getParentMetadata());
				modifyMetadata.setCode(newMetadata.getCode());

				modifyMetadata.setId(newMetadata.getId());
				modifyMetadata
						.setName((newMetadata.getName() == null || newMetadata.getName().equals("")) ? newMetadata
								.getCode() : newMetadata.getName());
				modifyMetadata.setClassifierId(newMetadata.getClassifierId());
				modifyMetadata.setStartTime(dbMetadata.getStartTime()); // 别忘了创建时间

				// 为了统一化处理,需要将没有变化的属性也写进来,否则入库的时候,就会把没放进来的属性设置为null -_-!!
				Map<Operation, Map<String, String>> modifyAttrs = new HashMap<Operation, Map<String, String>>(1);
				modifyAttrs.put(Operation.CHANGELESS, newMetadata.getAttrs());
				modifyMetadata.setModifyAttrs(modifyAttrs);

				modifyMetadatas.add(modifyMetadata);
				// 缓存需要修改的元数据ID
				AdapterExtractorContext.getInstance().addNewMetadataId(dbMetadata.getId(),
						newMetadata.getClassifierId());
			}
		}

	}

	private ModifyMetadata cleanNotExistAttr(ModifyMetadata metadata, boolean isSameName) {
		String metaModelCode = metadata.getClassifierId();
		if (errorAttrs.containsKey(metaModelCode)) {
			// 该元模型存在有不存在于库表中的属性
			Map<Operation, Map<String, String>> modifyAttrs = metadata.getModifyAttrs();
			if (modifyAttrs != null) {
				Map<String, String> createAttrs = modifyAttrs.get(Operation.CREATE);
				if (createAttrs != null) {
					Set<String> errorMAttrs = errorAttrs.get(metaModelCode);
					for (String errorMAttr : errorMAttrs) {
						// 把库表中不存在的属性去除
						createAttrs.remove(errorMAttr);
					}
					if (createAttrs.size() == 0) {
						modifyAttrs.remove(Operation.CREATE);
					}
				}

			}
			if ((modifyAttrs == null || modifyAttrs.size() == 0 || (modifyAttrs.get(Operation.CREATE) == null && modifyAttrs
					.get(Operation.DELETE) == null))
					&& metadata.getMAttrs().size() == 0 && isSameName) {
				// 没有需要变动的属性,同时元数据的名称也相同
				return null;
			}
		}
		return metadata;
	}

	/**
	 * 为需要添加的元数据列表生成ID,同时将这些需要添加的元数据ID缓存起来,供以后比较使用。<br>
	 * ID生成方式：<s>从库表中的SEQUENCE获取,SEQUENCE缓存20,则程序自己造20个,完了再从库表中获取</s><br>
	 * 通过UUID生成32位长度字符串。
	 * 
	 * @param createMetaModel
	 *            需要添加的元模型
	 * @param createMetadatas
	 *            需要添加的元数据列表
	 */
	private void genCreateMetaModel(MMMetaModel createMetaModel, List<AbstractMetadata> createMetadatas) {
		for (AbstractMetadata createMetadata : createMetadatas) {
			createMetadata.setId(sequenceDao.getUuid());
			// 缓存需要添加的元数据ID
			AdapterExtractorContext.getInstance().addNewMetadataId(createMetadata.getId(),
					createMetadata.getClassifierId());
		}
		createMetaModel.setMetadatas(createMetadatas);
		createMetaModel.setHasMetadata(true);

	}

	private void checkMetaModel(MMMetadata newMetadata, Map<String, String> mAttrs) {
		Map<String, String> dAttrs = newMetadata.getAttrs();
		for (Iterator<String> iter = dAttrs.keySet().iterator(); iter.hasNext();) {
			String metaModelAttr = iter.next();
			if (!mAttrs.containsKey(metaModelAttr)) {
				// 元数据的属性不存在于元模型的属性中
				String metaModelCode = newMetadata.getClassifierId();
				if (errorAttrs.containsKey(metaModelCode)) {
					errorAttrs.get(metaModelCode).add(metaModelAttr);
				}
				else {
					Set<String> attrs = new HashSet<String>();
					attrs.add(metaModelAttr);
					errorAttrs.put(metaModelCode, attrs);
				}
			}
		}

	}

	/**
	 * 对当层元数据进行排序
	 * 
	 * @author qhchen
	 * @version 1.0 Date: Sep 30, 2009
	 */
	private static class MetadataComparator implements Comparator<AbstractMetadata> {

		/**
		 * 相同模型,不用比较模型
		 * 
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(AbstractMetadata o1, AbstractMetadata o2) {
			// 比较父结点是否一样
			int compareInt = o1.getParentMetadata().getId().compareTo(o2.getParentMetadata().getId());
			if (compareInt == 0) {
				// 如为相同父结点,则再比较本结点的code
				compareInt = o1.getCode().compareTo(o2.getCode());
			}
			return compareInt;
		}

	}

	public void setMetaModelDao(IMetaModelDao metaModelDao) {
		this.metaModelDao = metaModelDao;
	}

}
