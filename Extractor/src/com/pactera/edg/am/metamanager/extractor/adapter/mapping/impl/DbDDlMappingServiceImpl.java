 package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcDependency;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.sqlparser.dbobj.ColAliasMap;
import com.pactera.edg.am.sqlparser.dbobj.ColumnObj;
import com.pactera.edg.am.sqlparser.exception.MDSException;
import com.pactera.edg.am.sqlparser.oracle.OracleParserLoader;
import com.pactera.edg.am.sqlparser.oracle.ParseException;

public class DbDDlMappingServiceImpl extends BaseMappingServiceImpl implements
		IMetadataMappingService {
	private Log log = LogFactory.getLog(DbDDlMappingServiceImpl.class);

	/**
	 * DB类型
	 */
	private String dbType;

	/**
	 * 文件目录
	 */
	private String dbDDLDirectory;

	// 解析加载，负责从数据库中访问已有的元数据
	private OracleParserLoader oracleLoader = new OracleParserLoader();

	// 存放转换后的元数据,key为bo的id
	private Map<String, MMMetadata> metaDataMap = new HashMap<String, MMMetadata>();

	// 元数据依赖关系
	private List<MMDDependency> dDependencies = new ArrayList<MMDDependency>();

	// 依赖关系Map
	private Set<String> dependencySet = new HashSet<String>();

	public void metadataMapping(AppendMetadata aMetadata) throws Exception {
		// 文件路径
		String path = AdapterExtractorContext.getInstance().getDsAbsolutePath();
		File f = new File(path);
		// String pre = f.getName().substring(f.getName().lastIndexOf(".") + 1,
		// f.getName().length());
		log.info(new StringBuilder("开始解析DB的DDL文件,文件绝对路径:").append(path)
				.append(" ,...").toString());
		AdapterExtractorContext.addExtractorLog(
				ExtractorLogLevel.INFO,
				new StringBuilder("开始解析DB的DDL文件,文件绝对路径:").append(path)
						.append(" ,...").toString());

		try {

			MMMetaModel catalogMetaModel = singleParse(new FileInputStream(
					new File(path)), aMetadata, path);

			Set<MMMetaModel> aMetaModels = new HashSet<MMMetaModel>(1);
			aMetaModels.addAll(catalogMetaModel.getChildMetaModels());
			aMetadata.setChildMetaModels(aMetaModels);

			aMetadata.setDDependencies(dDependencies);
			log.info("文件解析完成!");
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO,
					"文件解析完成!");
		} catch (Exception e) {
			log.error("DDL文件解析失败, 文件绝对路径:" + path, e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"DDL文件解析失败, 文件绝对路径:" + path);
			throw e;
		}
	}

	private MMMetaModel singleParse(InputStream inputStream,
			AppendMetadata aMetadata, String path) throws Exception {

		/*
		 * 解析文件
		 */
		log.info("开始解析DDL文件");
		long date1 = System.currentTimeMillis();

		List<String> statements = matchParentheses("CREATE", inputStream);

		ColAliasMap colAliasMap = sqlParser(statements);

		long date2 = System.currentTimeMillis();
		log.info("解析完成，耗时：" + (date2 - date1) / 1000 + "秒");

		/*
		 * 转换BO
		 */
		log.info("开始转换结果集");
		long date3 = System.currentTimeMillis();

		// 工程元模型
		MMMetaModel catalogMetaModel = aMetadata.getMetaModel();

		// 工程元数据
		MMMetadata catalogProject = aMetadata.getMetadata();

		mappingDbDDl(catalogMetaModel, catalogProject, colAliasMap);

		Set<MMMetaModel> aMetaModels = new HashSet<MMMetaModel>(1);
		aMetaModels.addAll(catalogMetaModel.getChildMetaModels());
		aMetadata.setChildMetaModels(aMetaModels);

		aMetadata.setDDependencies(dDependencies);

		long date4 = System.currentTimeMillis();
		log.info("转换完成，耗时：" + (date4 - date3) / 1000 + "秒");

		return catalogMetaModel;
	}

	/**
	 * 解析DDL语句
	 * 
	 * @param sql
	 * @return
	 */
	private ColAliasMap sqlParser(List<String> sqlList) {
		try {
			if ("Oracle".equals(dbType)) {
				oracleLoader.parseDDLs(sqlList, null);
			}
		} catch (ParseException e) {
			AdapterExtractorContext
					.addSQLParserLog(ExtractorLogLevel.WARN, "错误信息：" + "cause:"
							+ e.getCause() + "msg:" + e.getMessage());
			log.info("cause:" + e.getCause() + "msg:" + e.getMessage());
		} catch (MDSException e) {
			AdapterExtractorContext
					.addSQLParserLog(ExtractorLogLevel.WARN, "错误信息：" + "cause:"
							+ e.getCause() + "msg:" + e.getMessage());
			log.info("cause:" + e.getCause() + "msg:" + e.getMessage());
		} catch (Exception e) {
			AdapterExtractorContext
					.addSQLParserLog(ExtractorLogLevel.WARN, "错误信息：" + "cause:"
							+ e.getCause() + "msg:" + e.getMessage());
			log.info("cause:" + e.getCause() + "msg:" + e.getMessage());
		} catch (Error e) {
			AdapterExtractorContext
					.addSQLParserLog(ExtractorLogLevel.WARN, "错误信息：" + "cause:"
							+ e.getCause() + "msg:" + e.getMessage());
			log.info("cause:" + e.getCause() + "msg:" + e.getMessage());
		}

		// 解析结果集
		ColAliasMap colAliasMap = oracleLoader.getColAliasMap();

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

		return colAliasMap;
	}

	public void mappingDbDDl(MMMetaModel catalogMetaModel,
			MMMetadata catalogProject, ColAliasMap colAliasMap) {

		// 实例
		for (Iterator<String> it = colAliasMap.getCam().keySet().iterator(); it
				.hasNext();) {
			String key = it.next();
			List<ColumnObj> columnObjs = colAliasMap.getCam().get(key);
			for (int i = 0; i < columnObjs.size(); i++) {
				ColumnObj columnObj = columnObjs.get(i);
				String schemaCode = columnObj.getSchemaObj().getCode();

				// Schema元模型
				MMMetaModel schemaMm = catalogMetaModel
						.getChildMetaModel("Schema");

				if (schemaMm == null) {
					schemaMm = this.getMetaModel(catalogMetaModel, "Schema");
				}
				MMMetadata schemaMd = getMetaData(catalogProject, schemaMm,
						schemaCode, schemaCode, schemaCode);

				// ColumnSet
				String columnSetCode = columnObj.getColumnSetObj().getCode();
				int columnSetType = columnObj.getColumnSetObj()
						.getColumnSetType();

				MMMetaModel columnSetMm;
				if (columnSetType == 0) {
					// 表
					columnSetMm = schemaMm.getChildMetaModel("Table");
					if (columnSetMm == null) {
						columnSetMm = this.getMetaModel(schemaMm, "Table");
					}
				} else if (columnSetType == 1) {
					// 视图
					columnSetMm = schemaMm.getChildMetaModel("View");
					if (columnSetMm == null) {
						columnSetMm = this.getMetaModel(schemaMm, "View");
					}
				} else {
					log.error("错误的ColumnSet类型:" + columnSetType);
					continue;
				}

				MMMetadata columnSetMd = getMetaData(schemaMd, columnSetMm,
						columnSetCode, columnSetCode, schemaCode + "."
								+ columnSetCode);

				// 字段
				String columnCode = columnObj.getCode();
				MMMetaModel columnMm = columnSetMm.getChildMetaModel("Column");
				if (columnMm == null) {
					columnMm = this.getMetaModel(columnSetMm, "Column");
				}
				getMetaData(columnSetMd, columnMm, columnCode, columnCode,
						schemaCode + "." + columnSetCode + "." + columnCode);

			}
		}

		// 依赖关系
		for (Iterator<String> it = colAliasMap.getCam().keySet().iterator(); it
				.hasNext();) {
			String key = it.next();
			List<ColumnObj> columnObjs = colAliasMap.getCam().get(key);
			for (int i = 0; i < columnObjs.size(); i++) {
				ColumnObj columnObj = columnObjs.get(i);
				String schemaCode = columnObj.getSchemaObj().getCode();
				String columnSetCode = columnObj.getColumnSetObj().getCode();
				String columnCode = columnObj.getCode();
				List<ColumnObj> depColumnObjs = columnObj.getDepCols();
				for (int j = 0; j < depColumnObjs.size(); j++) {
					ColumnObj depColumnObj = depColumnObjs.get(j);
					String depSchemaCode = depColumnObj.getSchemaObj()
							.getCode();
					String depColumnSetCode = depColumnObj.getColumnSetObj()
							.getCode();
					String depColumnCode = depColumnObj.getCode();

					// 表级依赖关系
					String fromTableId = schemaCode + columnSetCode;
					String toTableId = depSchemaCode + depColumnSetCode;
					String tableDependencyKey = genDependencyKey(fromTableId,
							toTableId, null, null);
					if (!dependencySet.contains(tableDependencyKey)) {
						addDependency(metaDataMap.get(fromTableId), null,
								metaDataMap.get(toTableId), null);
						dependencySet.add(tableDependencyKey);
					}

					// 字段级
					String fromColumnId = fromTableId + columnCode;
					String toColumnId = toTableId + depColumnCode;
					String columnDependencyKey = genDependencyKey(fromColumnId,
							toColumnId, null, null);
					if (!dependencySet.contains(columnDependencyKey)) {
						addDependency(metaDataMap.get(fromColumnId), null,
								metaDataMap.get(toColumnId), null);
						dependencySet.add(columnDependencyKey);
					}

				}
			}
		}

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

	/**
	 * 得到一个元数据，包含名称和父元数据
	 * 
	 * @param parentMetadata
	 * @param metaDataName
	 * @return
	 */
	private MMMetadata getMetaData(MMMetadata parentMetadata, MMMetaModel mm,
			String metaDataCode, String metaDataName, String id) {
		MMMetadata mMMetadata = super.createMetadata(parentMetadata, mm,
				metaDataCode, metaDataName);
		metaDataMap.put(id, mMMetadata);
		return mMMetadata;
	}

	/**
	 * 生成依赖关系对象
	 * 
	 * @param modelDependencyMap
	 */
	private void addDependency(MMMetadata owner, String ownerRole,
			MMMetadata value, String valueRole) {
		MMDDependency mmd = new MMDDependency();
		mmd.setOwnerMetadata(owner);
		mmd.setValueMetadata(value);
		mmd.setOwnerRole(ownerRole);
		mmd.setValueRole(valueRole);
		dDependencies.add(mmd);
	}

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public String getDbDDLDirectory() {
		return dbDDLDirectory;
	}

	public void setDbDDLDirectory(String dbDDLDirectory) {
		this.dbDDLDirectory = dbDDLDirectory;
	}

	/**
	 * 
	 * @param inputStream
	 * @return
	 */
	private List<String> matchParentheses(String keyWord,
			InputStream inputStream) {
		BufferedReader in = new BufferedReader(new InputStreamReader(
				inputStream));

		// 所有信息
		String content = "";
		// 读取的行
		String line;
		try {
			while ((line = in.readLine()) != null) {
				content = content + line;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// 语句队列
		List<String> statementList = new ArrayList<String>();

		String[] statements = content.split("CREATE");

		for (int i = 1; i < statements.length; i++) {
			String statement = "CREATE" + getStatement(statements[i]);
			statement = StringUtils.remove(statement, "\"");
			log.info("statement:" + statement);
			statementList.add(statement);
		}

		return statementList;

	}

	/**
	 * 匹配括号，获取语句
	 * 
	 * @param statementList
	 * @return
	 */
	private String getStatement(String string) {
		// 如果是左括号加1，右括号减一
		int parenthesesCount = 0;
		char[] chars = string.toCharArray();

		for (int i = 0; i < chars.length; i++) {
			if ("(".equals("" + chars[i])) {
				parenthesesCount++;
			}

			if (")".equals("" + chars[i])) {
				parenthesesCount--;
				if (parenthesesCount == 0) {
					return string.substring(0, i + 1);
				}
			}

		}

		return string;
	}

	/**
	 * 生成依赖关系的KEY
	 * 
	 * @param pcDependency
	 * @return
	 */
	public static String genDependencyKey(String fromId, String toId,
			String fromRole, String toRole) {
		return "$[" + fromId + "]$$[" + fromRole + "]$$[" + toId + "]$$["
				+ toRole + "]$";
	}
}
