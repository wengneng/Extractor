package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofDependency;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.control.BofControl;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.control.ParseTypeHelper;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.webService.BofConnector;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.IDBDictionaryDao;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;
import com.pactera.edg.am.sqlparser.dbobj.ColAliasMap;
import com.pactera.edg.am.sqlparser.dbobj.ColumnObj;
import com.pactera.edg.am.sqlparser.exception.MDSException;
import com.pactera.edg.am.sqlparser.oracle.OracleParserLoader;
import com.pactera.edg.am.sqlparser.oracle.ParseException;

/**
 * Cognos转换器，负责Cognos数据的采集，及转换为cognos模型的数据
 * 
 * @author user
 * @version 1.0
 * 
 */
public class BiOfficeMappingServiceImpl extends BaseMappingServiceImpl
		implements IMetadataMappingService {
	private Log log = LogFactory.getLog(BiOfficeMappingServiceImpl.class);

	// 存放转换后的元数据,key为bo的id
	private Map<String, MMMetadata> metaDataMap = new HashMap<String, MMMetadata>();

	// 所有的依赖关系
	private Map<String, String> dependencyMap = new HashMap<String, String>();

	// 元数据依赖关系
	private List<MMDDependency> dDependencies = new ArrayList<MMDDependency>();

	private BofControl bofControl;

	private BofConnector bofConnector;

	// 解析加载，负责从数据库中访问已有的元数据
	private OracleParserLoader loader = new OracleParserLoader();

	private IDBDictionaryDao dbDictionary;

	/**
	 * 将Bioffice模型数据转换为元数据模型的数据
	 * 
	 * @param obj
	 * @return
	 */
	public void metadataMapping(AppendMetadata metadata) {
		dbDictionary = (IDBDictionaryDao) ExtractorContextLoader
				.getBean("dbDictionaryDao");
		loader.setDbDictionary(dbDictionary);
		long date1 = System.currentTimeMillis();
		log.info("开始登录bi.office");
		// 登陆bioffice
		// ClientConnector conn = bofConnector.getConnector();

		log.info("开始解析bi.office");
		// 采集
		bofControl = new BofControl(bofConnector);
		BofBO bofBO = bofControl.actionParse();

		// 采集完成，注销服务器
		// conn.close();

		long date2 = System.currentTimeMillis();

		log.info("解析完成，耗时：" + (date2 - date1) / 1000 + "秒");

		log.info("开始转换bi.office");
		// 工程元模型
		MMMetaModel respositoryMetaModel = metadata.getMetaModel();

		// 工程元数据
		MMMetadata respository = metadata.getMetadata();

		for (int i = 0; i < bofBO.getChildren().size(); i++) {
			mappingBiOffice(respositoryMetaModel, respository, bofBO
					.getChildren().get(i));
		}

		Set<MMMetaModel> aMetaModels = new HashSet<MMMetaModel>(1);
		aMetaModels.addAll(respositoryMetaModel.getChildMetaModels());
		metadata.setChildMetaModels(aMetaModels);

		// log.info("开始模型依赖关系转换");

		// 添加所有依赖关系
		mappingDependency(bofBO.getAllDependencys());

		// 依赖关系
		metadata.setDDependencies(dDependencies);

		long date3 = System.currentTimeMillis();

		log.info("转换完成，耗时：" + (date3 - date2) / 1000 + "秒");

	}

	public void mappingBiOffice(MMMetaModel parentMetaModel,
			MMMetadata parentMetadata, BofBO bofBO) {
		// isUnique(bofBO);
		// 如果没有元数据返回
		if (bofBO == null) {
			return;
		}

		// 子节点去重
		// unique(bofBO);

		// 构建表级间接依赖关系
		String reportType = ",SIMPLE_REPORT,DashboardMap,Dashboard,FREE_REPORT,METRIC_REPORT,";
		if (reportType.indexOf("," + bofBO.getType() + ",") != -1) {
			this.addTableIndirectlyDep(bofBO);
		}

		// 构建字段级间接依赖关系
		if ("REPORT_FIELD".equals(bofBO.getType())) {
			this.addColumnIndirectlyDep(bofBO);
		}

		// SQL解析
		if ("TEXT_BUSINESS_VIEW".equals(bofBO.getType())
				|| "RAWSQL_BUSINESS_VIEW".equals(bofBO.getType())) {
			String sql = bofBO.getAttributes().get("sql");
			String datasourceId = bofBO.getAttributes().get("datasourceId");
			sqlParser(bofBO, sql, datasourceId);
		}

		// 模型
		MMMetaModel mm = parentMetaModel.getChildMetaModel(ParseTypeHelper
				.getBofType(bofBO.getType()).getModelName());

		if (mm == null) {
			mm = this.getMetaModel(parentMetaModel, ParseTypeHelper.getBofType(
					bofBO.getType()).getModelName());
		}

		// 元数据
		MMMetadata md = this.getMetaData(parentMetadata, mm, bofBO.getCode(),
				bofBO.getName());

		// isMMUnique(mm);

		// 属性
		md.setAttrs(bofBO.getAttributes());

		// 临时存放转换后的对象
		metaDataMap.put(bofBO.getId(), md);

		// 递归
		for (int i = 0; i < bofBO.getChildren().size(); i++) {
			mappingBiOffice(mm, md, bofBO.getChildren().get(i));
		}

	}

	/**
	 * 转换所有BO的依赖关系
	 */
	private void mappingDependency(Map<String, BofDependency> dependencys) {
		for (Iterator iterator = dependencys.keySet().iterator(); iterator
				.hasNext();) {
			String key = (String) iterator.next();
			BofDependency dependency = dependencys.get(key);
			addDependency(metaDataMap.get(dependency.getFromId()), metaDataMap
					.get(dependency.getToId()), dependency.getFromId(),
					dependency.getToId());
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
		metaModel.setCode(mmName);
		metaModel.setParentMetaModel(parentMetaModel);

		// 对父模型进行设置
		parentMetaModel.setHasChildMetaModel(true);
		parentMetaModel.addMetaModel(metaModel);

		return metaModel;
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

	public BofConnector getBofConnector() {
		return bofConnector;
	}

	public void setBofConnector(BofConnector bofConnector) {
		this.bofConnector = bofConnector;
	}

	private boolean isUnique(BofBO bofBO) {
		if (bofBO.getParent() != null) {
			List<BofBO> children = bofBO.getParent().getChildren();
			for (int i = 0; i < children.size(); i++) {
				if (bofBO.getCode().equals(children.get(i).getCode())
						&& bofBO.getId() != children.get(i).getId()) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * 对子节点去重,不区分大小写
	 * 
	 * @param bofBO
	 * @return
	 */
	private void unique(BofBO bofBO) {
		List tmp = new ArrayList();
		List children = bofBO.getChildren();
		Map<String, String> map = new HashMap();
		for (Iterator it = children.iterator(); it.hasNext();) {
			BofBO bofBOtmp = (BofBO) it.next();
			// code与类型都相同
			if (map.get(bofBOtmp.getCode() + "@@" + bofBOtmp.getType()) != null) {
				tmp.add(bofBOtmp);
				log.info("重复的CODE:" + bofBOtmp.getCode() + ";id:"
						+ bofBOtmp.getId() + ";type:" + bofBOtmp.getType());
			} else {
				map.put(bofBOtmp.getCode() + "@@" + bofBOtmp.getType(),
						bofBOtmp.getId());
			}
		}

		for (int i = 0; i < tmp.size(); i++) {
			children.remove(tmp.get(i));
		}

	}

	/**
	 * 分析出SQL中用到的字段与查询字段的关系，并且获取这些字段在数据源中的ID
	 * 
	 * @param sql
	 * @return
	 */
	private void sqlParser(BofBO bofBO, String sql, String datasourceId) {
		// log
		log.info(genPathLog(bofBO));

		// 数据源
		BofBO datasourceBO = bofBO.getBofBOById(datasourceId);

		if (datasourceBO != null) {
			// 通过SQL解析器或者解析结果
			List<String> sqlList = new ArrayList<String>();
			sqlList.add(sql);
			log.info("SQL:" + sql);
			try {
				loader.parseSQLs(sqlList, datasourceBO.getAttributes().get(
						"user"));
			} catch (ParseException e) {
				AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
						genPathLog(bofBO) + ";错误信息：" + "cause:" + e.getCause()
								+ "msg:" + e.getMessage());
				log.info(genPathLog(bofBO) + ";cause:" + e.getCause() + "msg:"
						+ e.getMessage());
			} catch (MDSException e) {
				AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
						genPathLog(bofBO) + ";错误信息：" + "cause:" + e.getCause()
								+ "msg:" + e.getMessage());
				log.info(genPathLog(bofBO) + ";cause:" + e.getCause() + "msg:"
						+ e.getMessage());
			} catch (Exception e) {
				AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
						genPathLog(bofBO) + ";错误信息：" + "cause:" + e.getCause()
								+ "msg:" + e.getMessage());
				log.info(genPathLog(bofBO) + ";cause:" + e.getCause() + "msg:"
						+ e.getMessage());
			} catch (Error e) {
				AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
						genPathLog(bofBO) + ";错误信息：" + "cause:" + e.getCause()
								+ "msg:" + e.getMessage());
				log.info(genPathLog(bofBO) + ";cause:" + e.getCause() + "msg:"
						+ e.getMessage());
			}

			// 解析结果集
			ColAliasMap colAliasMap = loader.getColAliasMap();

			// 日志
			log.debug("colAliasMap.getCam():" + colAliasMap.getCam());
			for (Iterator<String> it = colAliasMap.getCam().keySet().iterator(); it
					.hasNext();) {
				String key = it.next();
				log.debug("key:" + key + "[");
				List<ColumnObj> columnObjs = colAliasMap.getCam().get(key);
				for (int i = 0; i < columnObjs.size(); i++) {
					ColumnObj columnObj = columnObjs.get(i);
					log.debug(columnObj.getSchemaObj().getCode() + "."
							+ columnObj.getColumnSetObj().getCode() + "."
							+ columnObj.getCode() + ";");
				}
				log.debug("]");
			}

			for (Iterator<String> it = colAliasMap.getCam().keySet().iterator(); it
					.hasNext();) {
				// 别名依赖的字段
				String key = it.next();
				String childKey = StringUtils.remove(key, "\"");
				BofBO businessFieldBO = bofBO.getChildByCodeNotCase(childKey,
						"\"");
				if (businessFieldBO == null) {
					log.debug("数据集：" + bofBO.getId() + ",找不到该别名:");
					log.debug("key:" + key);
					log.debug("childKey:" + childKey);
				} else {
					List<ColumnObj> columnObjs = colAliasMap.getCam().get(key);
					for (int i = 0; i < columnObjs.size(); i++) {
						// 依赖的字段信息
						ColumnObj columnObj = columnObjs.get(i);

						if (columnObj != null
								&& columnObj.getSchemaObj() != null
								&& columnObj.getColumnSetObj() != null) {

							// 该字段所对应的数据源的表
							BofBO tableBO = null;
							BofBO schemaBO = datasourceBO
									.getChildByCodeNotCase("DEFAULT");
							if (schemaBO == null) {
								if (null != datasourceBO
										.getChildByCodeNotCase(columnObj
												.getSchemaObj().getCode())) {
									tableBO = datasourceBO
											.getChildByCodeNotCase(
													columnObj.getSchemaObj()
															.getCode())
											.getChildByCodeNotCase(
													columnObj.getColumnSetObj()
															.getCode());
								}
							} else {
								tableBO = schemaBO
										.getChildByCodeNotCase(columnObj
												.getColumnSetObj().getCode());
							}

							// 表级依赖
							if (tableBO == null) {
								log
										.debug("找不到该表或者视图：schema:"
												+ columnObj.getSchemaObj()
														.getCode()
												+ "table or view:"
												+ columnObj.getColumnSetObj()
														.getCode());
							} else {
								bofBO.addDependency(tableBO.getId());
								// 字段级依赖
								if (null != tableBO
										.getChildByCodeNotCase(columnObj
												.getCode())) {
									businessFieldBO.addDependency(tableBO
											.getChildByCodeNotCase(
													columnObj.getCode())
											.getId());
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * 增加表级间接关系
	 * 
	 * @param bofBO
	 */
	private void addTableIndirectlyDep(BofBO bofBO) {
		HashSet<String> types = new HashSet<String>();
		types.add("BASEVIEW");
		types.add("BASETABLE");
		types.add("BASEPROCEDURE");
		bofBO.addIndirectlyDep(types, 7);
	}

	/**
	 * 增加字段级间接关系
	 * 
	 * @param bofBO
	 */
	private void addColumnIndirectlyDep(BofBO bofBO) {
		HashSet<String> types = new HashSet<String>();
		types.add("PROC_FIELD");
		types.add("FIELD");
		bofBO.addIndirectlyDep(types, 7);
	}

	private String genPathLog(BofBO bofBO) {
		return "解析的元数据路径为：" + bofBO.getContextPath() + "/" + bofBO.getCode();
	}
}
