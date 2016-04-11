package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl.ParseModelUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl.CognosConnector83;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl.Parse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl.ReportHarvestScope;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * Cognos转换器，负责Cognos数据的采集，及转换为cognos模型的数据
 * 
 * @author user
 * @version 1.0
 * 
 */
public class CognosMappingServiceImpl extends BaseMappingServiceImpl implements
		IMetadataMappingService {
	private Log log = LogFactory.getLog(CognosMappingServiceImpl.class);

	// 存放转换后的元数据,key为bo的id
	private Map<String, MMMetadata> metaDataMap = new HashMap<String, MMMetadata>();

	// 存放所有模型，key为模型在内容库中的查询路径
	private Map<String, CognosBO> modelMap = new HashMap<String, CognosBO>();

	// 存放所有报表，key为模型在内容库中的查询路径
	private List<CognosBO> reportList = new ArrayList<CognosBO>();

	// 供查询使用的bo
	private CognosBO searchBO = null;

	// 依赖关系map
	private Map<String, String> dependencyMap = new HashMap<String, String>();

	// 字段级模型
	private final static String COLUMN_METAMODEL = ",column,dataItem,queryItem,reportField,";

	/**
	 * 元数据依赖关系
	 */
	private List<MMDDependency> dDependencies = new ArrayList<MMDDependency>();

	private Parse pp;

	private ContentManagerService_Port cms;

	private CognosConnector83 cc;

	public ContentManagerService_Port getCms() {
		return cms;
	}

	public void setCms(ContentManagerService_Port cms) {
		this.cms = cms;
	}

	/**
	 * 读取采集范围
	 * 
	 * @return
	 */
	private CognosBO readHarvestSocpe() {
		ReportHarvestScope rhs = new ReportHarvestScope();
		CognosBO contentBO = rhs.loadCatalogFromServer(cms);
		return contentBO;
	}

	/**
	 * 将Cognos模型数据转换为元数据模型的数据
	 * 
	 * @param obj
	 * @return
	 */
	public void metadataMapping(AppendMetadata metadata) {
		log.info("开始cognos转换");
		// 登陆cognos
		this.cms = cc.connectByWebService();

		// 读取采集范围
		CognosBO cb = readHarvestSocpe();

		pp.parse(cb, cms);

		// 采集完成，注销cognos服务器
		cc.logOff();

		// 工程元模型
		MMMetaModel projectMetaModel = metadata.getMetaModel();

		// 工程元数据
		MMMetadata projectMetadata = metadata.getMetadata();

		log.info("开始cognos实例转换");

		log.info("开始模型依赖关系转换");

		for (int i = 0; i < cb.getChildren().size(); i++) {
			mappingCognos(projectMetaModel, projectMetadata, cb.getChildren()
					.get(i));
		}

		Set<MMMetaModel> aMetaModels = new HashSet<MMMetaModel>(1);
		aMetaModels.addAll(projectMetaModel.getChildMetaModels());
		metadata.setChildMetaModels(aMetaModels);

		// 添加模型依赖关系
		mappingModelDependency();

		// 添加报表依赖关系
		mappingReportDependency();

		// 添加直接关系
		addReportIndirectlyDep(cb);

		// 添加所有依赖关系
		mappingDependency(cb);

		// 依赖关系
		metadata.setDDependencies(dDependencies);

	}

	public void mappingCognos(MMMetaModel parentMetaModel,
			MMMetadata parentMetadata, CognosBO cb) {
		// 如果没有元数据返回
		if (cb == null) {
			return;
		}

		// 模型
		MMMetaModel mm = parentMetaModel.getChildMetaModel(this
				.getMMCode(getMMName(cb.getType())));
		if (mm == null) {
			mm = this.getMetaModel(parentMetaModel, getMMName(cb.getType()));
		}

		// 元数据
		MMMetadata md = this.getMetaData(parentMetadata, mm, cb.getCode(), cb
				.getName());

		// 属性
		md.setAttrs(cb.getAttributes());

		// 临时存放转换后的对象
		metaDataMap.put(cb.getId(), md);

		if ("model".equals(cb.getType())) {
			modelMap.put(cb.getSearchPath(), cb);
		}

		if ("report".equals(cb.getType())) {
			reportList.add(cb);
		}

		if ("subReport".equals(cb.getType())) {
			uniqueReport(cb);
		}

		// 递归
		for (int i = 0; i < cb.getChildren().size(); i++) {
			mappingCognos(mm, md, cb.getChildren().get(i));

		}

	}

	/**
	 * 去除重复的报表
	 * 
	 * @param reportBo
	 */
	private void uniqueReport(CognosBO reportBo) {
		List<CognosBO> reports = reportBo.getParent().getChildren();
		List<CognosBO> childernRep = new ArrayList<CognosBO>();
		for (Iterator iterator = reports.iterator(); iterator.hasNext();) {
			CognosBO report = (CognosBO) iterator.next();
			// 如果id不同，并且长度相同，就进行比较看是否是重复的报表
			if ((!reportBo.getId().equals(report.getId()))
					&& (reportBo.getChildren().size() == report.getChildren()
							.size())) {
				// 计算相同数
				int equalCount = 0;
				for (int j = 0; j < reportBo.getChildren().size(); j++) {
					for (int k = 0; k < report.getChildren().size(); k++) {
						// code相同，并且类型相同
						if (reportBo.getChildren().get(j).getCode().equals(
								report.getChildren().get(k).getCode())
								&& reportBo.getChildren().get(j).getType()
										.equals(
												report.getChildren().get(k)
														.getType())) {
							equalCount++;
						}
					}
				}
				// 有不同，不是重复报表
				if (equalCount != reportBo.getChildren().size()) {

					childernRep.add(report);
				}

			} else {
				childernRep.add(report);
			}
		}
		reportBo.getParent().setChildren(childernRep);
	}

	/**
	 * 转换模型依赖关系
	 */
	private void mappingModelDependency() {
		for (Iterator<String> iterator = modelMap.keySet().iterator(); iterator
				.hasNext();) {
			String key = (String) iterator.next();
			CognosBO cb = modelMap.get(key);
			Map<String, List<String>> depExpressions = cb.getDepExpressions();
			for (Iterator<String> iterator1 = depExpressions.keySet()
					.iterator(); iterator1.hasNext();) {
				String key1 = (String) iterator1.next();

				// 查询owner元数据对象
				CognosBO ownerBO = searchByExpressionFromModel(cb, key1);
				if (ownerBO != null) {
					List<String> expressions = (List<String>) depExpressions
							.get(key1);
					analysisDependency(ownerBO, expressions, cb);
				}
			}
		}
	}

	/**
	 * 添加依赖关系，对于被依赖端表级的依赖关系，需要细化到字段级别
	 */
	private void analysisDependency(CognosBO ownerBO, List<String> expressions,
			CognosBO modelBO) {
		List<CognosBO> ownerBOs = new ArrayList<CognosBO>();
		String ownerExp = ownerBO.getAttributes().get("expression");
		// 如果有表达式，判断表级或者字段级的
		if (ownerExp != null) {
			String[] expList = ParseModelUtil.analysisExpression(ownerExp);
			// 如果是表级的，在字段级建立依赖关系
			if (expList.length == 2) {
				ownerBOs = ownerBO.getChildren();
			} else if (expList.length == 3) {
				ownerBOs.add(ownerBO);
			}
		} else {
			ownerBOs.add(ownerBO);
		}

		// 循环所有依赖的表达式
		for (int i = 0; i < expressions.size(); i++) {
			// 查询value元数据对象
			CognosBO valueBO = searchByExpressionFromModel(modelBO, expressions
					.get(i));
			if (valueBO != null) {
				// MMMetadata value = metaDataMap.get(valueBO.getId());

				String expression = valueBO.getAttributes().get("expression");
				String[] expList = ParseModelUtil
						.analysisExpression(expression);

				List<CognosBO> valueBOs = new ArrayList<CognosBO>();

				// 如果是表级的表达式，依赖关系需要建立到字段级，所以取它的子孙建立依赖关系 如果是字段级表达式，加入到list中即可
				if (expList.length == 2) {
					valueBOs = valueBO.getChildren();
				} else if (expList.length == 3) {
					valueBOs.add(valueBO);
				}

				// 对依赖关系的被依赖段循环
				for (int j = 0; j < valueBOs.size(); j++) {
					valueBO = valueBOs.get(j);
					MMMetadata value = metaDataMap.get(valueBO.getId());

					// 对依赖关系的依赖端循环
					for (int k = 0; k < ownerBOs.size(); k++) {
						ownerBO = ownerBOs.get(k);
						// 如果BO都不为空
						if (ownerBO != null && valueBO != null) {
							// 如果依赖关系不重复，添加依赖关系
							// MMMetadata owner = null;
							// owner = metaDataMap.get(ownerBO.getId());
							// addDependency(owner, value, ownerBO.getId(),
							// valueBO.getId());
							// addTableDependency(ownerBO, valueBO);
							ownerBO.getDependency().add(valueBO);
						}
					}
				}

			}

		}
	}

	/**
	 * 在模型的BO中查询满足表达式的BO
	 * 
	 * @param modelBO
	 * @return
	 */
	private CognosBO searchByExpressionFromModel(CognosBO cb, String expression) {
		searchBO = null;
		searchModel(cb, expression);
		return searchBO;
	}

	// 递归
	private void searchModel(CognosBO cb, String expression) {
		if (expression != null && !"".equals(expression) && cb != null) {
			if (expression.equals(cb.getAttributes().get("expression"))) {
				searchBO = cb;
			} else {
				List<CognosBO> children = cb.getChildren();
				for (int i = 0; i < children.size(); i++) {
					searchModel(children.get(i), expression);
				}
			}

		}
	}

	/**
	 * 转换报表中的依赖关系
	 */
	private void mappingReportDependency() {
		for (int i = 0; i < reportList.size(); i++) {
			CognosBO reportBO = reportList.get(i);
			if (reportBO != null) {
				String modelPath = reportBO.getAttributes().get("modelPath");
				CognosBO modelBO = modelMap.get(modelPath);
				if (modelBO != null) {
					for (int j = 0; j < reportBO.getChildren().size(); j++) {
						CognosBO subReportBO = reportBO.getChildren().get(j);
						for (int k = 0; k < subReportBO.getChildren().size(); k++) {
							CognosBO attBO = subReportBO.getChildren().get(k);
							List<String> expressions = attBO
									.getDepExpressions().get(attBO.getCode());
							analysisDependency(attBO, expressions, modelBO);
						}

					}
				} else if (reportBO.getChildren() != null) {
					// log.error("找不到报表的模型，报表名称为：" + reportBO.getName());
				}
			}
		}
	}

	/**
	 * 转换所有BO的依赖关系
	 */
	private void mappingDependency(CognosBO cb) {
		List<CognosBO> dependency = cb.getDependency();
		for (int i = 0; i < dependency.size(); i++) {
			addDependency(metaDataMap.get(cb.getId()), metaDataMap
					.get(dependency.get(i).getId()), cb.getId(), dependency
					.get(i).getId());
			addTableDependency(cb, dependency.get(i));
		}

		for (int i = 0; i < cb.getChildren().size(); i++) {
			mappingDependency(cb.getChildren().get(i));
		}
	}

	/**
	 * 生成依赖关系对象
	 * 
	 * @param modelDependencyMap
	 */
	private void addDependency(MMMetadata owner, MMMetadata value,
			String ownerBOId, String valueBOId) {
		if (owner != null && value != null) {
			if (dependencyMap.get(ownerBOId + "-->" + valueBOId) == null) {
				MMDDependency mmd = new MMDDependency();
				mmd.setOwnerMetadata(owner);
				mmd.setValueMetadata(value);
				mmd.setOwnerRole("reportDep");
				mmd.setValueRole("reportDeped");
				dDependencies.add(mmd);
				dependencyMap.put(ownerBOId + "-->" + valueBOId, ownerBOId
						+ "-->" + valueBOId);
			}
		}

	}

	/**
	 * 得到一个元数据，包含名称和父元数据
	 * 
	 * @param parentMetadata
	 * @param metaDataName
	 * @return
	 */
	private MMMetadata getMetaData(MMMetadata parentMetadata, MMMetaModel mm,
			String metaDataCode, String metaDataName) {

		// MMMetadata md = new MMMetadata();
		// mm.setHasMetadata(true);
		// md.setCode(metaDataCode);
		// md.setName(metaDataName);
		// md.setParentMetadata(parentMetadata);
		//
		// parentMetadata.addChildMetadata(md);
		//		
		// md.setClassifierId(mm.getCode());

		return super.createMetadata(parentMetadata, mm, metaDataCode,
				metaDataName);
	}

	/**
	 * 得到一个元模型，包含名称，代码和父元模型
	 * 
	 * @param parentMetaModel
	 * @param clazz
	 * @return
	 */
	private MMMetaModel getMetaModel(MMMetaModel parentMetaModel, String mmName) {

		MMMetaModel metaModel = new MMMetaModel();
		metaModel.setName(mmName);
		metaModel.setCode(getMMCode(mmName));
		metaModel.setParentMetaModel(parentMetaModel);

		// 对父模型进行设置
		parentMetaModel.setHasChildMetaModel(true);
		parentMetaModel.addMetaModel(metaModel);

		return metaModel;
	}

	/**
	 * 得到bo对应的模型名称
	 * 
	 * @param clazz
	 * @return
	 */
	private String getMMName(String type) {
		String firstChar = type.substring(0, 1).toUpperCase();
		String lastString = type.substring(1, type.length());
		return firstChar + lastString;
	}

	/**
	 * 得到bo对应的模型名称
	 * 
	 * @param clazz
	 * @return
	 */
	private String getMMCode(String mmName) {
		return "Cognos8_" + mmName;
	}

	/**
	 * 添加表级依赖关系
	 */
	private void addTableDependency(CognosBO ownerBO, CognosBO valueBO) {
		if (COLUMN_METAMODEL.indexOf("," + ownerBO.getType() + ",") != -1
				&& COLUMN_METAMODEL.indexOf("," + valueBO.getType() + ",") != -1) {
			CognosBO ownerParent = ownerBO.getParent();
			CognosBO valueParent = valueBO.getParent();
			if (ownerParent != null && valueParent != null) {
				MMMetadata owner = metaDataMap.get(ownerParent.getId());
				MMMetadata value = metaDataMap.get(valueParent.getId());
				addDependency(owner, value, ownerParent.getId(), valueParent
						.getId());
			}
		}

	}

	// /**
	// * 增加表级间接关系
	// *
	// * @param cognosBO
	// */
	// private void addTableIndirectlyDep(CognosBO cognosBO) {
	// HashSet<String> types = new HashSet<String>();
	// types.add("columnSet");
	// types.add("storedProcedure");
	// cognosBO.addIndirectlyDep(types, 7);
	// }

	/**
	 * 增加报表与数据库的直接关系
	 * 
	 * @param cognosBO
	 */
	private void addReportIndirectlyDep(CognosBO cognosBO) {
		HashSet<String> types = new HashSet<String>();
		types.add("column");
		childernColumnIndirectlyDep(cognosBO, types);
	}

	/**
	 * 递归
	 * 
	 * @param cognosBO
	 */
	private void childernColumnIndirectlyDep(CognosBO cognosBO,
			HashSet<String> types) {
		if ("reportField".equals(cognosBO.getType())) {
			// 报表与数据库字段级的直接关系
			List<CognosBO> depList = new ArrayList<CognosBO>();
			cognosBO.addIndirectlyDep(types, 7, depList);
			// 建立报表与数据库表级之间的关系
			CognosBO reportBO = cognosBO.getParent().getParent();
			for (int i = 0; i < depList.size(); i++) {
				reportBO.getDependency().add(depList.get(i).getParent());
			}
		}
		for (int i = 0; i < cognosBO.getChildren().size(); i++) {
			this.childernColumnIndirectlyDep(cognosBO.getChildren().get(i),
					types);
		}
	}

	public Map<String, MMMetadata> getMetaDataMap() {
		return metaDataMap;
	}

	public void setMetaDataMap(Map<String, MMMetadata> metaDataMap) {
		this.metaDataMap = metaDataMap;
	}

	public List<MMDDependency> getDDependencies() {
		return dDependencies;
	}

	public void setDDependencies(List<MMDDependency> dependencies) {
		dDependencies = dependencies;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}

	public Parse getPp() {
		return pp;
	}

	public void setPp(Parse pp) {
		this.pp = pp;
	}

	public CognosConnector83 getCc() {
		return cc;
	}

	public void setCc(CognosConnector83 cc) {
		this.cc = cc;
	}

	public Map<String, CognosBO> getModelMap() {
		return modelMap;
	}

	public void setModelMap(Map<String, CognosBO> modelMap) {
		this.modelMap = modelMap;
	}

	public CognosBO getSearchBO() {
		return searchBO;
	}

	public void setSearchBO(CognosBO searchBO) {
		this.searchBO = searchBO;
	}

	public Map<String, String> getDependencyMap() {
		return dependencyMap;
	}

	public void setDependencyMap(Map<String, String> dependencyMap) {
		this.dependencyMap = dependencyMap;
	}

	public List<CognosBO> getReportList() {
		return reportList;
	}

	public void setReportList(List<CognosBO> reportList) {
		this.reportList = reportList;
	}

}
