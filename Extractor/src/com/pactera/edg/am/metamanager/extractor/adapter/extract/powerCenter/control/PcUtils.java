package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control;

import java.util.ArrayList;
import java.util.List;

import org.jdom.Element;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcDependency;

public class PcUtils {

	/**
	 * 生成XPath
	 * 
	 * @param parentId
	 * @param name
	 * @return
	 */
	public static String genXPath(Element element) {
		String xpath = "";
		do {
			String currentPath = "/" + element.getName();
			if (element.getAttributeValue("NAME") != null
					&& element.getAttributeValue("NAME").length() > 0) {
				currentPath = currentPath + "[@name="
						+ element.getAttributeValue("NAME") + "]";
			}
			xpath = currentPath + xpath;

			if (!element.isRootElement()) {
				element = element.getParentElement();
			}
		} while (!element.isRootElement());

		return xpath;
	}

	/**
	 * 根据Element的标签名以及属性NAME的值查询
	 * 
	 * @param element
	 * @param name
	 * @return
	 */
	public static Element getChildByName(Element element, String elementName,
			String nameValue, String dbdName) {
		List<Element> elements = element.getChildren(elementName);
		for (int i = 0; i < elements.size(); i++) {
			if (nameValue.equals(elements.get(i).getAttributeValue("NAME"))) {
				if (dbdName == null
						|| (dbdName != null && elements.get(i)
								.getAttributeValue("DBDNAME").equals(dbdName))) {
					return elements.get(i);
				}

			}
		}
		return null;
	}
	
	/**
	 * 生成依赖关系的KEY
	 * 
	 * @param pcDependency
	 * @return
	 */
	public static String genDependencyKey(PcDependency pcDependency) {
		return "$[" + pcDependency.getFromId() + "]$$["
				+ pcDependency.getFromRole() + "]$$[" + pcDependency.getToId()
				+ "]$$[" + pcDependency.getToRole() + "]$";
	}
	
	/**
	 * 获取源或者目标的字段级BO
	 * 
	 * @param pcBO
	 */
	public static List<PcBO> getFieldBO(PcBO pcBO, String fieldName) {
		List<PcBO> reList = new ArrayList<PcBO>();
		List<PcBO> children = pcBO.getChildren();
		// 可能有多个数据库或者多个文件
		if (children.size() > 0) {
			if (children.get(0).getType().equals("CATALOG")) {
				for (int i = 0; i < children.size(); i++) {
					PcBO catalogBO = children.get(i);
					for (int j = 0; j < catalogBO.getChildren().size(); j++) {
						PcBO schemaBO = catalogBO.getChildren().get(j);
						for (int k = 0; k < schemaBO.getChildren().size(); k++) {
							PcBO columnSetBO = schemaBO.getChildren().get(k);
							reList.add(columnSetBO.searchChildByCodeAndType(
									fieldName, "DBCOLUMN"));
						}

					}
				}
			} else if (children.get(0).getType().equals("FLATFILE")) {
				for (int i = 0; i < children.size(); i++) {
					PcBO flatFileBO = children.get(i);
					reList.add(flatFileBO.searchChildByCodeAndType(fieldName,
							"FLATFILECOLUMN"));
				}
			}
		}
		return reList;
	}
}
