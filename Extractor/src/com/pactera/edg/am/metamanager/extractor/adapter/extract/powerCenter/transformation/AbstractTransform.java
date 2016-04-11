package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcUtils;

public class AbstractTransform {
	private static Log log = LogFactory.getLog(AbstractTransform.class);

	/**
	 * 解析transformation的依赖关系
	 * 
	 * @param instanceBO
	 *            要解析依赖关系的BO
	 * @param element
	 *            该BO的element
	 */
	public void genInstanceDependency(PcBO instanceBO, Element element) {

		// instance 的定义
		Element depElement = instanceBO.getDepElement();

		String type = element.getAttributeValue("TYPE");
		try {
			if ("TRANSFORMATION".equals(type)) {
				/*
				 * input与output之间的关系
				 */

				// 找出所有的input(PORTTYPE="INPUT/OUTPUT"、PORTTYPE="INPUT")
				List<Element> inputFieldElements = (List<Element>) XPath
						.selectNodes(depElement,
								"./TRANSFORMFIELD[contains(@PORTTYPE, 'INPUT')]");

				for (int i = 0; i < inputFieldElements.size(); i++) {
					Element inputFieldElement = inputFieldElements.get(i);
					String fieldName = inputFieldElement
							.getAttributeValue("NAME");

					// 查询表达式中使用到该input的output
					Set<String> depFieldList = new HashSet<String>();
					// 如果有表达式，寻找依赖关系，如果没有并且它是input/output类型的，取本身
					getDepOutput(fieldName, inputFieldElement, depFieldList);

					/*
					 * 建立关系
					 */
					// 查询到该input往前关联的output字段
					List<Element> connectors = (List<Element>) XPath
							.selectNodes(element, "../CONNECTOR[@TOFIELD='"
									+ fieldName + "' and @TOINSTANCE='"
									+ element.getAttributeValue("NAME") + "']");

					for (int j = 0; j < connectors.size(); j++) {
						String fromInstanceName = connectors.get(j)
								.getAttributeValue("FROMINSTANCE");
						String fromFieldName = connectors.get(j)
								.getAttributeValue("FROMFIELD");
						PcBO fromInstanceBO = instanceBO.getParent()
								.searchChildByCodeAndType(fromInstanceName,
										"TRANSFORMATIONINSTANCE");

						if (fromInstanceBO != null) {
							PcBO fromInstanceFieldBO = fromInstanceBO
									.searchChildByCodeAndType(fromFieldName,
											"TRANSFORMFIELDINSTANCE");

							// output
							for (Iterator<String> iterator = depFieldList
									.iterator(); iterator.hasNext();) {
								String outputName = (String) iterator.next();
								PcBO outputBO = instanceBO
										.searchChildByCodeAndType(outputName,
												"TRANSFORMFIELDINSTANCE");
								// 建立source与target关系
								outputBO.addDependency(fromInstanceFieldBO
										.getId(), "source");
								fromInstanceFieldBO.addDependency(outputBO
										.getId(), "target");
							}
						}
					}

				}

				// 处理源和目标的依赖关系，直接通过CONNECTOR关联，不用分析表达式
			} else {
				List<Element> connectors = new ArrayList<Element>();
				if ("SOURCE".equals(type)) {
					connectors = (List<Element>) XPath.selectNodes(element,
							"../CONNECTOR[@FROMINSTANCE='"
									+ element.getAttributeValue("NAME") + "']");
				} else if ("TARGET".equals(type)) {
					connectors = (List<Element>) XPath.selectNodes(element,
							"../CONNECTOR[@TOINSTANCE='"
									+ element.getAttributeValue("NAME") + "']");
				}

				for (int i = 0; i < connectors.size(); i++) {
					Element connector = connectors.get(i);
					String fromInstanceName = connector
							.getAttributeValue("FROMINSTANCE");
					String fromInstanceFieldName = connector
							.getAttributeValue("FROMFIELD");
					String toInstanceName = connector
							.getAttributeValue("TOINSTANCE");
					String toInstanceFieldName = connector
							.getAttributeValue("TOFIELD");

					// 依赖关系
					if ("SOURCE".equals(type)) {
						PcBO fromInstanceBO = instanceBO.getParent()
								.searchChildByCodeAndType(fromInstanceName,
										"SOURCEINSTANCE");
						PcBO toFieldBO = instanceBO.getParent()
								.searchChildByCodeAndType(toInstanceName,
										"TRANSFORMATIONINSTANCE")
								.searchChildByCodeAndType(toInstanceFieldName,
										"TRANSFORMFIELDINSTANCE");

						List<PcBO> fromFieldBOList = PcUtils.getFieldBO(
								fromInstanceBO, fromInstanceFieldName);
						for (int j = 0; j < fromFieldBOList.size(); j++) {
							toFieldBO.addDependency(fromFieldBOList.get(j)
									.getId(), "source");

						}
					} else if ("TARGET".equals(type)) {
						PcBO fromInstanceBO = instanceBO.getParent()
								.searchChildByCodeAndType(fromInstanceName,
										"TRANSFORMATIONINSTANCE");
						if (fromInstanceBO != null) {
							PcBO fromFieldBO = fromInstanceBO
									.searchChildByCodeAndType(
											fromInstanceFieldName,
											"TRANSFORMFIELDINSTANCE");

							PcBO toInstanceBO = instanceBO.getParent()
									.searchChildByCodeAndType(toInstanceName,
											"TARGETINSTANCE");

							List<PcBO> toFieldBOList = PcUtils.getFieldBO(
									toInstanceBO, toInstanceFieldName);

							for (int j = 0; j < toFieldBOList.size(); j++) {
								fromFieldBO.addDependency(toFieldBOList.get(j)
										.getId(), "target");
							}
						}
					}

				}
			}
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 根据expression的信息查询出该字段被哪些output字段依赖
	 * 
	 * @param fieldName
	 * @param elements
	 * @param depFieldList
	 * @param maxLevel
	 *            递归最大层次
	 * @param level
	 * @return
	 */
	protected void getDepOutput(String fieldName, Element fieldlement,
			Set<String> depFieldList) {
		getDepOutputRecursion(fieldName, fieldlement, depFieldList, 7, 1);
	}

	/**
	 * 递归
	 * 
	 * @param fieldName
	 * @param fieldElement
	 * @param depFieldList
	 * @param maxLevel
	 * @param level
	 * @return
	 */
	private void getDepOutputRecursion(String fieldName, Element fieldElement,
			Set<String> depFieldList, int maxLevel, int level) {
		if (level < maxLevel) {
			level++;
			for (int i = 0; i < fieldElement.getParentElement().getChildren(
					"TRANSFORMFIELD").size(); i++) {
				Element child = (Element) fieldElement.getParentElement()
						.getChildren("TRANSFORMFIELD").get(i);
				// 匹配这样一个字符串：字段名的前后都不能是数字、字母、下划线
				String expression = child.getAttributeValue("EXPRESSION");
				String name = child.getAttributeValue("NAME");
				String portType = child.getAttributeValue("PORTTYPE");
				if (expression != null) {
					Pattern p = Pattern.compile("(?<!\\w)" + fieldName
							+ "(?!\\w)");
					Matcher m = p.matcher(expression);
					if (m.find()) {
						// 如果端口类型为输出类型，需要记录依赖关系
						if (portType != null
								&& (portType.indexOf("OUTPUT") != -1)) {
							depFieldList.add(name);
						}

						// 继续递归查询
						getDepOutputRecursion(name, fieldElement, depFieldList,
								maxLevel, level);

					}
					// 如果没有表达式，并且是同一个字段，且PORTTYPE='INPUT/OUTPUT'，添加依赖关系
				} else if (fieldName.equals(name)
						&& "INPUT/OUTPUT".equals(portType)) {
					depFieldList.add(name);
				}
			}
		}
	}
}
