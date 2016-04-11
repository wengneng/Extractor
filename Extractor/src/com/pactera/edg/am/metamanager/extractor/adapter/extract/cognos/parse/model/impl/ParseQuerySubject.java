package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl.ParseUtil;

public class ParseQuerySubject extends AModel {
	private Log log = LogFactory.getLog(ParseModel.class);

	public ParseQuerySubject(Document xmlDoc, String defaultLocal, Namespace ns) {
		this.xmlDoc = xmlDoc;
		this.defaultLocal = defaultLocal;
		this.ns = ns;

	}

	/**
	 * 解析查询主题
	 * 
	 * @param xmlDoc
	 * @param xpath
	 * @param currentNameSpace
	 * @return
	 */
	public CognosBO parseQuerySubject(CognosBO cb, Element el,
			Map<String, CognosBO> dataSourceMap) {
		String querySubjectType = "";
		try {
			querySubjectType = this.judgeQuerySubjectType(el);
			// 解析过滤器的依赖关系
			// String refXpath =
			// "./default:definition/default:dbQuery/default:filters/default:filterDefinition/default:refobj";
			// ParseModelUtil.parseRefObject(xmlDoc, refXpath, expression,
			// , ns);

			// 根据查询主题的类型，分别进行处理
		} catch (Exception e) {
			// log.error("无法判断查询主题的类型");
			// // TODO Auto-generated catch block
			// e.printStackTrace();
		}
		if ("dbQuery".equals(querySubjectType)) {
			// 类型
			try {
				parseQuerySubjectFromDB(cb, el, dataSourceMap);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.info(" 解析查询主题异常");
				e.printStackTrace();
			}
		}

		if ("storedProcedure".equals(querySubjectType)) {
			// 类型
			try {
				parseQuerySubjectFromProcedure(cb, el, dataSourceMap);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.info(" 解析查询主题异常");
				e.printStackTrace();
			}
		}

		return cb;
	}

	/**
	 * 解析数据库表来源的查询主题
	 * 
	 * @param xmlDoc
	 * @param xpath
	 * @return
	 * @throws Exception
	 */
	public CognosBO parseQuerySubjectFromDB(CognosBO cb, Element el,
			Map<String, CognosBO> dataSourceMap) throws Exception {
		String tableExpression = this.queryTableNameForQuerySubject(el);
		String[] tableList = ParseModelUtil.analysisExpression(tableExpression);

		// 取查询主题的表名作为code，为目录映射提供数据
		if (tableList.length > 1) {
			String dataSourceName = tableList[0];
			String tableName = tableList[1];
			CognosBO schemaBO = dataSourceMap.get(dataSourceName);
			if (schemaBO != null) {
				CognosBO columnSetBO = schemaBO.queryChildrenByCodeAndType(
						tableName, "columnSet");
				if (columnSetBO == null) {
					columnSetBO = new CognosBO(schemaBO);
					columnSetBO.setCode(tableName);
					columnSetBO.setName(tableName);
					columnSetBO.setType("columnSet");
					schemaBO.getChildren().add(columnSetBO);
				}
				// 查询主题与依赖关系
				cb.getDependency().add(columnSetBO);
			} else {
				throw new Exception("缺少数据源信息");
			}
		}
		return cb;
	}

	/**
	 * 解析存储过程来源的查询主题
	 * 
	 * @param xmlDoc
	 * @param xpath
	 * @return
	 * @throws Exception
	 */
	public CognosBO parseQuerySubjectFromProcedure(CognosBO cb, Element el,
			Map<String, CognosBO> dataSourceMap) throws Exception {

		Element dataSourceRefEl = ParseUtil
				.getChildElement(
						el,
						"./default:definition/default:storedProcedure/default:dataSourceRef",
						ns);

		Element canonicalNameEl = ParseUtil
				.getChildElement(
						el,
						"./default:definition/default:storedProcedure/default:canonicalName",
						ns);

		if (dataSourceRefEl != null && canonicalNameEl != null) {
			// 关联数据源的表达式，例如：[].[dataSources].[edwbview]
			String dataSourceRef = dataSourceRefEl.getValue();
			// 存储过程名
			String procuedureName = canonicalNameEl.getValue();
			// 分析出数据库名
			String[] dataSourceRefList = ParseModelUtil
					.analysisExpression(dataSourceRef);

			// 所在的schema的名字
			if (dataSourceRefList.length > 2) {
				String schemaName = dataSourceRefList[2].toUpperCase();
				// 查询是否已经解析了schema
				CognosBO schemaBO = dataSourceMap.get(schemaName);
				if (schemaBO != null) {
					CognosBO procuedureBO = schemaBO
							.queryChildrenByCodeAndType(procuedureName,
									"storedProcedure");
					if (procuedureBO == null) {
						procuedureBO = new CognosBO(schemaBO);
						procuedureBO.setCode(procuedureName);
						procuedureBO.setName(procuedureName);
						procuedureBO.setType("storedProcedure");

						schemaBO.getChildren().add(procuedureBO);

						// 存储过程参数
						List<Element> procParameterEls = ParseUtil
								.getChildrenElement(
										el,
										"./default:definition/default:storedProcedure/default:procParameters/default:procParameter",
										ns);
						if (procParameterEls.size() > 0) {
							for (int i = 0; i < procParameterEls.size(); i++) {
								CognosBO parameterBO = new CognosBO(
										procuedureBO);
								parameterBO.setCode(procParameterEls.get(i)
										.getChildText("parameterName", ns));
								parameterBO.setName(procParameterEls.get(i)
										.getChildText("parameterName", ns));
								parameterBO.setType("parameter");
								// 数据类型
								parameterBO.getAttributes().put(
										"datatype",
										procParameterEls.get(i).getChildText(
												"datatype", ns));

								// 精度
								parameterBO.getAttributes().put(
										"precision",
										procParameterEls.get(i).getChildText(
												"precision", ns));

								// 范围
								parameterBO.getAttributes().put(
										"scale",
										procParameterEls.get(i).getChildText(
												"scale", ns));

							}
						}
					}
					// 查询主题与依赖关系
					cb.getDependency().add(procuedureBO);
				} else {
					throw new Exception("缺少数据源信息");
				}
			}
		}

		return cb;
	}

	/**
	 * 判断查询主题的类型(来源是数据库和来源是模型) 如果具有数据库卡查询标签<dbQuery>的查询主题为数据库来源的查询主题 如果具有模型查询标签<modelQuery>的查询主题为模型来源的查询主题
	 * 
	 * @param xmlDoc
	 * @param xpath
	 * @return
	 * @throws Exception
	 */
	public String judgeQuerySubjectType(Element el) throws Exception {

		// 数据库查询的标签
		Element dbQueryEl = ParseUtil.getChildElement(el,
				"./default:definition/default:dbQuery", ns);

		// 模型查询标签
		Element modelQueryEl = ParseUtil.getChildElement(el,
				"./default:definition/default:modelQuery", ns);

		// 存储过程查询标签
		Element storedProcedureEl = ParseUtil.getChildElement(el,
				"./default:definition/default:storedProcedure", ns);

		if (dbQueryEl != null) {
			return "dbQuery";
		} else if (modelQueryEl != null) {
			return "modelQuery";
		} else if (storedProcedureEl != null) {
			return "storedProcedure";
		} else {
			throw new Exception("无法判断查询主题的类型");
		}

	}

	/**
	 * 解析当前数据库来源的查询主题的表名，例如：[gosl].[product]
	 * 
	 * @param xpath
	 * @return
	 */
	public String queryTableNameForQuerySubject(Element el) {
		// 查询table标签
		Element tableEl = ParseUtil
				.getChildElement(
						el,
						"./default:definition/default:dbQuery/default:sql/default:table",
						ns);

		String tableName = null;

		if (tableEl != null) {
			// 有<table>标签直接取表名
			tableName = tableEl.getText();
		} else {

			// 没有<table>标签取sql语句from后的表名
			String sql = ParseUtil.getChildElement(el,
					"./default:definition/default:dbQuery/default:sql", ns)
					.getText().toUpperCase();

			String sqlTemp = sql.substring(sql.lastIndexOf("FROM") + 4);

			// 去开头的回车和空格
			while (sqlTemp.startsWith("\n") || sqlTemp.startsWith(" ")) {
				sqlTemp = sqlTemp.substring(1, sqlTemp.length());
			}

			// 如果SQL是以表名结尾的直接赋值，否则截取字符串
			if (sqlTemp.indexOf(" ") != -1) {
				sqlTemp = sqlTemp.substring(0, sqlTemp.indexOf(" "));
			}
			if (sqlTemp.indexOf("\n") != -1) {
				sqlTemp = sqlTemp.substring(0, sqlTemp.indexOf("\n"));
			}

			tableName = sqlTemp;

		}

		return tableName.toUpperCase();
	}

	public Document getXmlDoc() {
		return xmlDoc;
	}

	public void setXmlDoc(Document xmlDoc) {
		this.xmlDoc = xmlDoc;
	}

	public String getDefaultLocal() {
		return defaultLocal;
	}

	public void setDefaultLocal(String defaultLocal) {
		this.defaultLocal = defaultLocal;
	}

	public Namespace getNs() {
		return ns;
	}

	public void setNs(Namespace ns) {
		this.ns = ns;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}
}
