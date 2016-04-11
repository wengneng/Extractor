package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.parse.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcUtils;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.parse.AbstractParse;

public class InstanceParse extends AbstractParse {

	private static Log log = LogFactory.getLog(InstanceParse.class);

	/**
	 * 解析Instance
	 * 
	 * @param parent
	 * @param element
	 * @param varMap
	 *            外部变量替换
	 * @return
	 */
	public PcBO parse(PcBO parent, Element element, Map<String, String> varMap) {
		String instanceTypes = ",TRANSFORMATION,MAPPLET,SOURCE,TARGET,";

		PcBO pcBO = null;
		if (instanceTypes
				.indexOf("," + element.getAttributeValue("TYPE") + ",") != -1) {
			pcBO = new PcBO(parent, element, null);

			// 组装基本信息
			bulidBaseInfoFromElement(pcBO, element);
			pcBO.setType(element.getAttributeValue("TYPE") + element.getName());

			// 解析属性
			buildAttribute(pcBO, element);

			// 添加依赖关系
			buildDependency(pcBO, element);

			// 装箱Instance的子孙节点
			parseInstanceChildren(pcBO, element, varMap);
		}
		return pcBO;
	}

	/**
	 * 装箱Instance的子孙节点
	 * 
	 * @param parent
	 * @param element
	 */
	private void parseInstanceChildren(PcBO parent, Element element,
			Map<String, String> varMap) {
		// log.info("parseInstanceChildren");

		if (parent.getDepElement() != null) {
			// 获取依赖的Element
			Element depedElementparent = parent.getDepElement();

			// 解析源和目标
			String sourceAndTarget = ",SOURCE,TARGET,";
			if (sourceAndTarget.indexOf("," + element.getAttributeValue("TYPE")
					+ ",") != -1) {

				// 判断是数据库还是接口文件
				String databaseType = depedElementparent
						.getAttributeValue("DATABASETYPE");

				String dbType = ",Oracle,Teradata,";
				String flatFileType = ",Flat File,";

				if (dbType.indexOf("," + databaseType + ",") != -1) {
					buildDB(parent, element, depedElementparent, varMap);
				}

				if (flatFileType.indexOf("," + databaseType + ",") != -1) {
					buildFlatFile(parent, element, depedElementparent);
				}

			}

			// 解析转换
			String transformation = ",TRANSFORMATION,";
			if (transformation.indexOf("," + element.getAttributeValue("TYPE")
					+ ",") != -1) {
				buildTransformation(parent, element, depedElementparent);
			}
		}
	}

	/**
	 * 装箱数据库类型的信息
	 * 
	 * @param parent
	 * @param element
	 *            instance
	 * @param depedElement
	 *            依赖的source或者target
	 */
	private void buildDB(PcBO parent, Element element, Element depedElement,
			Map<String, String> varMap) {
		// Mapping的名字
		String mappingName = parent.getParent().getCode();

		// 分为源和目标两种情况
		String type = element.getAttributeValue("TYPE");

		String name = element.getAttributeValue("NAME");
		String sqInstanceName;
		try {

			if ("SOURCE".equals(type)) {
				// 查询引用该Source Definition的Source Qualifier的名字
				// 查询关联的标签，例如：<ASSOCIATED_SOURCE_INSTANCE NAME="CDBEFF_SM_PARA"
				// />

				Element associateElement = (Element) XPath.selectNodes(
						element,
						"../INSTANCE/ASSOCIATED_SOURCE_INSTANCE[@NAME='" + name
								+ "']").get(0);

				// Source Qualifier的实例名
				sqInstanceName = associateElement.getParentElement()
						.getAttributeValue("NAME");
			} else {
				sqInstanceName = name;
			}

			// 查询所用用到该Mapping的session,从中提取运行态信息
			List<Element> sessionElements = (List<Element>) XPath.selectNodes(
					element.getParent(), "//SESSION[@MAPPINGNAME='"
							+ mappingName + "']");

			// 提取每条session中的数据库信息
			for (int i = 0; i < sessionElements.size(); i++) {
				Element sessionElement = sessionElements.get(i);

				String connectionName = getConnection(sqInstanceName,
						sessionElement);

				/*
				 * schema信息
				 */
				String schemaName = getSchema(name, sessionElement,
						connectionName);

				// 对schema信息进行拆分，分出catalog
				String[] strList = StringUtils.split(varMap.get(connectionName
						.toUpperCase()), "/");

				if (strList != null && strList.length > 1) {

					String catalogName = strList[0];

					// 如果没有指定schema，用用户名作为schema
					if (schemaName == null) {
						schemaName = strList[1];
					}

					PcBO catalogBO = parent.searchChildByCodeAndType(
							catalogName, "CATALOG");

					if (catalogBO == null) {
						// 装箱catalog
						catalogBO = new PcBO(parent, null, null);
						catalogBO.setCode(catalogName);
						catalogBO.setName(catalogName);
						catalogBO.setType("CATALOG");

					}

					PcBO schemaBO = catalogBO.searchChildByCodeAndType(
							schemaName, "SCHEMA");

					if (schemaBO == null) {
						// 装箱schema(如需要替换值，记录)，
						schemaBO = new PcBO(catalogBO, null, null);
						schemaBO.setCode(schemaName);
						schemaBO.setName(schemaName);
						schemaBO.setType("SCHEMA");
					}

					/*
					 * 表级
					 */
					String tableName = getTableName(name, sessionElement,
							depedElement);

					PcBO tableBO = schemaBO.searchChildByCodeAndType(tableName,
							"COLUMNSET");

					if (tableBO == null) {
						tableBO = new PcBO(schemaBO, null, null);
						tableBO.setCode(tableName);
						tableBO.setName(tableName);
						tableBO.setType("COLUMNSET");

						/*
						 * 字段
						 */
						for (int j = 0; j < depedElement.getChildren().size(); j++) {
							// sourceField的名字作为字段名，并且依赖于它，可查看详细信息
							Element columnElement = (Element) depedElement
									.getChildren().get(j);
							PcBO columnBO = new PcBO(tableBO, null, null);
							columnBO.setCode(columnElement
									.getAttributeValue("NAME"));
							columnBO.setName(columnElement
									.getAttributeValue("NAME"));
							columnBO.setType("DBCOLUMN");
							columnBO.setElementDependency(columnElement);
						}
					}
				} else {
					log.error("没有配置该connection的参数值,connection="
							+ connectionName);
				}
			}

		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 根据instance的信息，找到使用该instance的session连接名
	 * 
	 * @param element
	 * @return
	 * @throws JDOMException
	 */
	private String getConnection(String instanceName, Element sessionElement)
			throws JDOMException {
		// 查询用到的connection名
		List<Element> connectionRefElements = (List<Element>) XPath
				.selectNodes(sessionElement,
						"./SESSIONEXTENSION[@SINSTANCENAME='" + instanceName
								+ "']/CONNECTIONREFERENCE");

		if (connectionRefElements != null && connectionRefElements.size() > 0) {
			return connectionRefElements.get(0).getAttributeValue(
					"CONNECTIONNAME");
		}

		return null;
	}

	/**
	 * 查询schema的信息
	 * 
	 * @param instanceName
	 * @param sessionElement
	 * @param connectionName
	 * @param varMap
	 * @return
	 * @throws JDOMException
	 */
	private String getSchema(String instanceName, Element sessionElement,
			String connectionName) throws JDOMException {
		List<Element> schemaElements = (List<Element>) XPath.selectNodes(
				sessionElement, "./SESSTRANSFORMATIONINST[@SINSTANCENAME='"
						+ instanceName + "']/ATTRIBUTE[@NAME ='Owner Name']");

		// 如果有指定schemaName使用这个名字，否则使用connection的用户名作为schemaName
		String schemaName = null;
		if (schemaElements != null && schemaElements.size() > 0) {
			schemaName = schemaElements.get(0).getAttributeValue("VALUE");
		}
		return schemaName;
	}

	/**
	 * 获取表级信息
	 * 
	 * @param instanceName
	 * @param sessionElement
	 * @param depedElement
	 * @return
	 * @throws JDOMException
	 */
	private String getTableName(String instanceName, Element sessionElement,
			Element depedElement) throws JDOMException {
		List<Element> tableElement = (List<Element>) XPath
				.selectNodes(
						sessionElement,
						"./SESSTRANSFORMATIONINST[@SINSTANCENAME='"
								+ instanceName
								+ "' and @TRANSFORMATIONTYPE='Source Definition']/ATTRIBUTE[@NAME ='Source Table Name']");
		// 如果有指定源表，使用该指定的源表名，否则使用SOURCE的名字
		String tableName;
		if (tableElement != null && tableElement.size() > 0) {
			tableName = tableElement.get(0).getAttributeValue("VALUE");
		} else {
			tableName = depedElement.getAttributeValue("NAME");
		}
		return tableName;
	}

	/**
	 * 装箱文件类型的信息，还需要结合参数解析文件的路径
	 * 
	 * @param parent
	 * @param element
	 * @param depedElement
	 *            依赖的source或者target
	 */
	private void buildFlatFile(PcBO parent, Element element,
			Element depedElement) {

		// 将依赖的source的信息作为文件的信息
		PcBO flatFileBO = new PcBO(parent, null, null);
		bulidBaseInfoFromElement(flatFileBO, depedElement);
		flatFileBO.setType("FLATFILE");

		// 文件字段级信息
		List<Element> childrenElements = depedElement.getChildren();
		for (int i = 0; i < childrenElements.size(); i++) {
			Element childElement = childrenElements.get(i);
			PcBO flatFileColumnBO = new PcBO(flatFileBO, null, null);
			bulidBaseInfoFromElement(flatFileColumnBO, childElement);
			flatFileColumnBO.setType("FLATFILECOLUMN");
		}
	}

	/**
	 * 装箱转换的字段级信息
	 * 
	 * @param parent
	 * @param depedElementparent
	 */
	private void buildTransformation(PcBO parent, Element element,
			Element depedElementparent) {
		List<Element> children = depedElementparent
				.getChildren("TRANSFORMFIELD");

		/*
		 * 转换字段级信息
		 */
		for (int i = 0; i < children.size(); i++) {
			// transformationField的名字作为字段名，并且依赖于它，可查看详细信息
			Element fieldElement = (Element) children.get(i);
			PcBO fieldBO = new PcBO(parent, element, "/TRANSFORMFIELD[@NAME='"
					+ fieldElement.getAttributeValue("NAME") + "']");
			fieldBO.setCode(fieldElement.getAttributeValue("NAME"));
			fieldBO.setName(fieldElement.getAttributeValue("NAME"));
			fieldBO.setType("TRANSFORMFIELDINSTANCE");
			fieldBO.setElementDependency(fieldElement);
		}
	}
}
