package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jdom.Element;
import org.jdom.Namespace;

public final class ParseModelUtil {
	/**
	 * 解析依赖关系，把依赖关系保存在依赖关系数组中(key为依赖的对象，value为被依赖的对象的数组)
	 * 
	 * @param ownerExpression
	 * @param valueExpression
	 */
	public static void parseDependency(String ownerExpression,
			String valueExpression, Map<String, List<String>> ddvMap) {

		List<String> depList = ddvMap.get(ownerExpression);

		if (depList == null) {
			depList = new ArrayList<String>();
		}

		if (!depList.contains(valueExpression)) {
			depList.add(valueExpression);
		}

		ddvMap.put(ownerExpression, depList);

	}

	/**
	 * 对如：[gosl].[product].[product_id]进行解析 解析出{gosl,product,product_id}
	 * 
	 * @param expression
	 * @return
	 */
	public static String[] analysisExpression(String expression) {
		String[] expressionList = StringUtils.split(expression, "].");
		for (int i = 0; i < expressionList.length; i++) {

			int flag1 = expressionList[i].lastIndexOf("[");

			// 如果有[的符号，去掉
			if (flag1 != -1) {
				expressionList[i] = expressionList[i].substring(flag1+1,
						expressionList[i].length());
			}
			
			int flag2 = expressionList[i].lastIndexOf("]");
			
			// 如果有]的符号，去掉
			if (flag2 != -1) {
				expressionList[i] = expressionList[i].substring(0,
						flag2);
			}
		}

		return expressionList;
	}

	/**
	 * 解析xpath路径下的依赖关系，并且保存在依赖关系数组中
	 * 
	 * @param xmlDoc
	 * @param xpath
	 * @param dataPath
	 * @param ddvMap
	 * @param namespace
	 *            TODO
	 */
	public static List<String> parseRefObject(Element el, Namespace namespace) {
		List<String> refExpressions = new ArrayList<String>();

		// 解析依赖关系
		List<Element> refOjectEls = el.getChildren("refobj", namespace);

		for (int i = 0; i < refOjectEls.size(); i++) {

			Element refObjectEl = refOjectEls.get(i);

			String refExpression = refObjectEl.getText();

			// 添加依赖关系
			refExpressions.add(refExpression);
		}
		return refExpressions;
	}

	/**
	 * 找出默认地区化的名称的值
	 * 
	 * @param element
	 * @return
	 */
	public static String queryLocaleName(Element element, String defaultLocal,
			Namespace ns) {

		// 默认的地区化
		List nameElList = element.getChildren("name", ns);
		for (int j = 0; j < nameElList.size(); j++) {

			Element nameEl = (Element) nameElList.get(j);

			String locale = nameEl.getAttributeValue("locale");

			if (defaultLocal.equals(locale)) {
				return nameEl.getText();
			}
		}
		return null;
	}
}
