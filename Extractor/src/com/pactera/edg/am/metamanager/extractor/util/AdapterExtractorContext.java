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

package com.pactera.edg.am.metamanager.extractor.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleCover;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLog;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.ServerDomain;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.log.ExtractorLogObservable;

/**
 * 适配器与采集模块共有的上下文信息
 * 
 * @author 陈汉清
 * @version 1.0 Date: Aug 21, 2009
 * @author fbchen 增加数据源ID 2010-01-15
 */
public class AdapterExtractorContext {

	private static Log log = LogFactory.getLog(AdapterExtractorContext.class);

	private static AdapterExtractorContext context;

	private AdapterExtractorContext() {
		// 设置设置值大小
		setMaxMetadataCodeSize(Constants.MAX_METADATA_CODE_SIZE);
		setMaxMetadataNameSize(Constants.MAX_METADATA_NAME_SIZE);
		setMaxMetadataAttrSize(Constants.MAX_ALTER_METADATA_ATTR_SIZE);
		setMaxLogSize(Constants.MAX_LOG_SIZE);
	}

	/**
	 * 采集的悬挂结点元数据
	 */
	private AppendMetadata aMetadata;

	/**
	 * 是否需要采集多次流程
	 */
	private boolean needMultiExtract;

	private Map<String,TemplateTitleCover> isCover = new HashMap<String, TemplateTitleCover>();
	
	public Map<String, TemplateTitleCover> getIsCover() {
		return isCover;
	}

	public void setIsCover(String clsId,TemplateTitleCover title) {
		this.isCover.put(clsId,title);
	}

	/**
	 * 需要添加的,修改的,没有变动的元数据ID列表
	 */
	private Map<String, String> newMetadatasId = new HashMap<String, String>();

	/**
	 * 需要添加的,没有变动的元数据依赖关系列表
	 */
	private List<MMDDependency> newDependencies = new ArrayList<MMDDependency>();

	/**
	 * 依赖关系CODE的缓存
	 */
	private Map<String, String> relationCodeCache = new HashMap<String, String>(
			0);

	/**
	 * 数据源的绝对路径
	 */
	private String dsAbsolutePath;

	/**
	 * 数据源的目录
	 */
	private String dsDirectory;

	/**
	 * 是否写入审核表
	 */
	private boolean needAudit;

	/**
	 * 是全量式比较还是增量式比较
	 */
	private boolean fullIncrementCompare;

	/**
	 * 转换,适配器所采用的SPRING配置文件
	 */
	private String[] mapperConfs;

	/**
	 * 所需的参数
	 */
	private Properties parameters;

	/**
	 * 元模型的RMI接口
	 */
	private IClassifierQueryBS iClassifier;

	/**
	 * 任务实例ID
	 */
	private String taskInstanceId;

	/**
	 * 数据源ID
	 */
	private String datasourceId;

	/**
	 * 缓存元模型的属性
	 */
	private Map<String, Map<String, String>> metaModelsAttrs = new HashMap<String, Map<String, String>>();

	/**
	 * 缓存元模型的组合关系
	 */
	private Map<String, String> metaModelsComp = new HashMap<String, String>();

	/**
	 * 采集日志的观察者
	 */
	private ExtractorLogObservable eObservable;

	/**
	 * 采集日志是否可用
	 */
	private boolean extractorLoggerUseable;

	/**
	 * 操作采集的用户ID
	 */
	private String userId;

	/**
	 * 全局的时间
	 */
	private long globalTime;

	/**
	 * 采集返回的参数
	 */
	private ServerDomain serverDomain;

	// /**
	// * 回滚历史变更记录操作，包括历史元数据表，历史元数据组合关系表，历史元数据依赖关系表，默认为false
	// */
	// private boolean callbackHistoryOperation = false;
	// /**
	// * 回滚元数据记录操作,包括元数据表,默认为false
	// */
	// private boolean callbackMetadata = false;

	/**
	 * 元数据属性大小；元数据名称大小；元数据CODE大小;元数据记录日志的大小：从数据库中获取这些值
	 */
	private int maxMetadataAttrSize = -1, maxMetadataNameSize = -1,
			maxMetadataCodeSize = -1, maxLogSize = -1;

	/**
	 * 判断是否为文件上传方式
	 */
	private boolean isFileUpload = false;
	
	public boolean isFileUpload() {
		return isFileUpload;
	}

	public void setFileUpload(boolean isFileUpload) {
		this.isFileUpload = isFileUpload;
	}

	/**
	 * 判断是否集采适配器
	 */
	private boolean isDBExtract;

	/**
	 * 采集返回的状态
	 */
	private String returnStatus;

	public String getReturnStatus() {
		return returnStatus;
	}

	public void setReturnStatus(String returnStatus) {
		this.returnStatus = returnStatus;
	}

	public void genGlobalTime() {
		globalTime = System.currentTimeMillis();
	}

	public void setIsDBExtract(boolean isDBExtract) {
		this.isDBExtract = isDBExtract;
	}

	public boolean getIsDBExtract() {
		return this.isDBExtract;
	}

	public long getGlobalTime() {
		return globalTime;
	}

	public boolean isExtractorLoggerUseable() {
		return extractorLoggerUseable;
	}

	public void setExtractorLoggerUseable(boolean extractorLoggerUseable) {
		this.extractorLoggerUseable = extractorLoggerUseable;
	}

	public ExtractorLogObservable getEObservable() {
		return eObservable;
	}

	public void setEObservable(ExtractorLogObservable observable) {
		eObservable = observable;
	}

	public void addMetaModelAttrs(String metaModelCode,
			Map<String, String> attrs) {
		metaModelsAttrs.put(metaModelCode, attrs);
	}

	public void addRelationCode(String key, String code) {
		relationCodeCache.put(key, code);
	}

	public void addMetaModelComp(String metaModelCode, String compedRelation) {
		metaModelsComp.put(metaModelCode, compedRelation);
	}

	public void addDependency(MMDDependency dependency) {
		newDependencies.add(dependency);
	}

	public void setNewDependency(List<MMDDependency> dependencies) {
		this.newDependencies = dependencies;
	}

	public IClassifierQueryBS getIClassifier() {
		return iClassifier;
	}

	public void setIClassifier(IClassifierQueryBS classifier) {
		iClassifier = classifier;
	}

	public static AdapterExtractorContext getInstance() {
		if (context == null) {
			context = new AdapterExtractorContext();
		}
		return context;
	}

	/**
	 * 写采集日志
	 * 
	 * @param level
	 *            日志信息的级别
	 * @param logType
	 *            日志类型
	 * @param message
	 *            日志信息
	 */
	public static void addExtractorLog(ExtractorLogLevel level,
			ExtractorLog.LogType logType, String message) {
		if (getInstance().isExtractorLoggerUseable()) {
			getInstance().getEObservable().addExtractorLog(
					new ExtractorLog(level, logType, message));
		}
	}

	/**
	 * 写采集日志
	 * 
	 * @param level
	 *            日志信息的级别
	 * @param message
	 *            日志信息
	 */
	public static void addExtractorLog(ExtractorLogLevel level, String message) {
		addExtractorLog(level, ExtractorLog.LogType.EXTRACTOR, message);
	}

	/**
	 * 写SQL解析日志
	 * 
	 * @param level
	 *            日志信息的级别
	 * @param message
	 *            日志信息
	 */
	public static void addSQLParserLog(ExtractorLogLevel level, String message) {
		addExtractorLog(level, ExtractorLog.LogType.SQLPARSER, message);
	}

	/**
	 * 一次采集任务中的一个采集流程完结之后,的清理工作(一次采集任务中可以有多个采集流程)
	 */
	public void singleExtractClear() {

		dsAbsolutePath = null;
		// if(!fullIncrementCompare){
		aMetadata.clearCache();
		// }

	}

	/**
	 * 一次采集任务的完结之后,的清理工作
	 */
	public void multiExtractClear() {
		aMetadata = null;
		needMultiExtract = false;
		globalTime = 0;

		fullIncrementCompare = false;
		if (parameters != null)
			parameters.clear();
		mapperConfs = null;
		newMetadatasId.clear();

		iClassifier = null;
		metaModelsAttrs.clear();
		metaModelsComp.clear();
		newDependencies.clear();

		relationCodeCache.clear();
		// 不清理之??
		// taskInstanceId = -1;
		dsDirectory = null;
		// logType = null;
		returnStatus = null;

		try {
			// 关闭数据源连接(务必关闭!!)
			ExtractorContextLoader.getDataSource().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void finallyClear() {
		needAudit = false;
		taskInstanceId = null;
		userId = null;
	}

	public AppendMetadata getAMetadata() {
		return aMetadata;
	}

	public void setAMetadata(AppendMetadata dsMetadata) {
		this.aMetadata = dsMetadata;
	}

	public boolean isNeedAudit() {
		return needAudit;
	}

	public void setNeedAudit(boolean needAudit) {
		this.needAudit = needAudit;
	}

	public boolean isFullIncrementCompare() {
		return fullIncrementCompare;
	}

	public void setFullIncrementCompare(boolean fullIncrementCompare) {
		this.fullIncrementCompare = fullIncrementCompare;
	}

	public String[] getMapperConfs() {
		return mapperConfs;
	}

	public void setMapperConfs(String[] mapperConfs) {
		this.mapperConfs = mapperConfs;
	}

	public Properties getParameters() {
		return parameters;
	}

	public void setParameters(Properties parameters) {
		this.parameters = parameters;
	}

	public void print() {
		log.info(new StringBuilder("needAudit:").append(needAudit)
				.append(",fullIncrementCompare:").append(fullIncrementCompare)
				.append(",Spring").append(Arrays.toString(mapperConfs))
				.append(",\nParameter").append(parameters.toString())
				.toString());
	}

	public Map<String, String> getNewMetadatasId() {
		return newMetadatasId;
	}

	public void addNewMetadataId(String newMetadataId, String metaModelCode) {
		newMetadatasId.put(newMetadataId, metaModelCode);
	}

	public String getDsAbsolutePath() {
		return dsAbsolutePath;
	}

	public void setDsAbsolutePath(String dsAbsolutePath) {
		this.dsAbsolutePath = dsAbsolutePath;
	}

	public boolean isNeedMultiExtract() {
		return needMultiExtract;
	}

	public void setNeedMultiExtract(boolean needMultiExtract) {
		this.needMultiExtract = needMultiExtract;
	}

	public Map<String, Map<String, String>> getMetaModelsAttrs() {
		return metaModelsAttrs;
	}

	public Map<String, String> getMetaModelsComp() {
		return metaModelsComp;
	}

	public String getTaskInstanceId() {
		return taskInstanceId;
	}

	public void setTaskInstanceId(String taskInstanceId) {
		this.taskInstanceId = taskInstanceId;
	}

	public List<MMDDependency> getNewDependencies() {
		return newDependencies;
	}

	public Map<String, String> getRelationCodeCache() {
		return relationCodeCache;
	}

	public String getDsDirectory() {
		return dsDirectory;
	}

	public void setDsDirectory(String dsDirectory) {
		this.dsDirectory = dsDirectory;
	}

	public void setDatasourceId(String datasourceId) {
		this.datasourceId = datasourceId;
	}

	public String getDatasourceId() {
		return datasourceId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public ServerDomain getServerDomain() {
		return serverDomain;
	}

	public void setServerDomain(ServerDomain serverDomain) {
		this.serverDomain = serverDomain;
	}

	public int getMaxMetadataAttrSize() {
		return maxMetadataAttrSize;
	}

	public void setMaxMetadataAttrSize(int maxMetadataAttrSize) {
		this.maxMetadataAttrSize = maxMetadataAttrSize;
	}

	public int getMaxMetadataNameSize() {
		return maxMetadataNameSize;
	}

	public void setMaxMetadataNameSize(int maxMetadataNameSize) {
		this.maxMetadataNameSize = maxMetadataNameSize;
	}

	public int getMaxMetadataCodeSize() {
		return maxMetadataCodeSize;
	}

	public void setMaxMetadataCodeSize(int maxMetadataCodeSize) {
		this.maxMetadataCodeSize = maxMetadataCodeSize;
	}

	public int getMaxLogSize() {
		return maxLogSize;
	}

	public void setMaxLogSize(int maxLogSize) {
		this.maxLogSize = maxLogSize;
	}

}
