package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.report.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.AttributeBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.ReportTypeBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl.ParseUtil;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class ParseReportLayOut {
	private Document reportDoc;
	private Namespace ns;
	private Map<String, List<String>> queryDependncyMap;
	private Log log = LogFactory.getLog(ParseReportLayOut.class);
	private Map<String, String> reportTypes = new HashMap<String, String>();

	/**
	 * 初始化报表类型
	 */
	public ParseReportLayOut() {
		reportTypes.put("list", "列表");
		reportTypes.put("crosstab", "交叉表");
		reportTypes.put("repeaterTable", "重复器表");
		reportTypes.put("repeater", "重复器");
		reportTypes.put("mapChart", "地图");
		reportTypes.put("pieChart", "图表");
		reportTypes.put("paretoChart", "图表");
		reportTypes.put("progressiveChart", "图表");
		reportTypes.put("scatterChart", "图表");
		reportTypes.put("bubbleChart", "图表");
		reportTypes.put("bubbleChart", "图表");
		reportTypes.put("threeDScatterChart", "图表");
		reportTypes.put("radarChart", "图表");
		reportTypes.put("polarChart", "图表");
		reportTypes.put("gaugeChart", "图表");
		reportTypes.put("metricsChart", "图表");
		reportTypes.put("polarChart", "图表");
		reportTypes.put("polarChart", "图表");
		reportTypes.put("polarChart", "图表");

	}

	/**
	 * 解析报表以及它的依赖关系
	 * 
	 * @param reportVO
	 * @param queryDependncyMap
	 * @return
	 */
	public List<CognosBO> parse(Document reportDoc,
			Map<String, List<String>> queryDependncyMap, CognosBO parent) {

		this.reportDoc = reportDoc;
		this.ns = reportDoc.getRootElement().getNamespace();
		this.queryDependncyMap = queryDependncyMap;

		return parseSubReports(parent);
	}

	/**
	 * 解析报表中的子报表
	 * 
	 * @param reportBO
	 * @return
	 */
	private List<CognosBO> parseSubReports(CognosBO reportBO) {
		// 保存子报表Elements
		Map<Element, List<Element>> subReports = new HashMap<Element, List<Element>>();

		// 查询所有有引用查询属性的节点
		List<Attribute> reportFieldAttributeEls = ParseUtil.getChildrenElement(
				getReportDoc(),
				"/default:report/default:layouts//@refDataItem", getNs());
		for (int i = 0; i < reportFieldAttributeEls.size(); i++) {
			Element reportFieldElement = reportFieldAttributeEls.get(i)
					.getParent();
			Element subReportElement = findSubReportElement(reportFieldElement);

			// 子报表节点存在
			if (subReportElement != null) {
				List<Element> reportFields = subReports.get(subReportElement);
				if (reportFields == null) {
					reportFields = new ArrayList<Element>();
					subReports.put(subReportElement, reportFields);
				}
				reportFields.add(reportFieldElement);
			}
		}

		// 组装BO
		List<CognosBO> subReportBOs = new ArrayList<CognosBO>();
		int i = 1;
		for (Iterator iterator = subReports.keySet().iterator(); iterator
				.hasNext();) {
			Element subReportElement = (Element) iterator.next();
			if (reportTypes.get(subReportElement.getName()) != null) {
				CognosBO rb = new CognosBO(reportBO);
				rb.setCode("subReport_" + i);
				rb.setName(reportTypes.get(subReportElement.getName()));
				rb.setType("subReport");
				rb.getAttributes()
						.put("reportType", subReportElement.getName());
				subReportBOs.add(rb);

				// 查询值
				String queryName = subReportElement
						.getAttributeValue("refQuery");

				List<Element> reportFields = subReports.get(subReportElement);
				Map<String, String> fieldMap = new HashMap<String, String>();
				int k = 1;
				for (int j = 0; j < reportFields.size(); j++) {
					Element reportField = reportFields.get(j);
					String fieldName = reportField
							.getAttributeValue("refDataItem");
					if (fieldMap.get(fieldName) == null) {
						CognosBO ab = new CognosBO(rb);
						rb.getChildren().add(ab);
						ab.setCode("reportField" + (k + 1));
						ab.setName(fieldName);
						ab.setType("reportField");

						String refDataItemValue = reportField
								.getAttributeValue("refDataItem");

						// 字段级依赖关系

						String[] expList = { queryName, refDataItemValue };
						List<String> dependencyModel = queryDependncyMap
								.get(ParseUtil.buildExpression(expList));

						ab.getDepExpressions().put(ab.getCode(),
								dependencyModel);
						fieldMap.put(fieldName, fieldName);
						k++;
					}
				}

				i++;
			}

		}
		return subReportBOs;
	}

	/**
	 * 根据查询报表字段查询报表节点
	 * 
	 * @param reportFieldElement
	 * @return
	 */
	private Element findSubReportElement(Element reportFieldElement) {
		Element currentElement = reportFieldElement;
		String attributeValue = null;
		// refQuery属性无值
		while (attributeValue == null || attributeValue.length() == 0) {
			// 如果是根节点
			if (currentElement.isRootElement()) {
				return null;
			} else {
				currentElement = currentElement.getParentElement();
				attributeValue = currentElement.getAttributeValue("refQuery");
			}
		}
		return currentElement;

	}

	public Document getReportDoc() {
		return reportDoc;
	}

	public void setReportDoc(Document reportDoc) {
		this.reportDoc = reportDoc;
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

	public Map<String, List<String>> getQueryDependncyMap() {
		return queryDependncyMap;
	}

	public void setQueryDependncyMap(Map<String, List<String>> queryDependncyMap) {
		this.queryDependncyMap = queryDependncyMap;
	}
}
