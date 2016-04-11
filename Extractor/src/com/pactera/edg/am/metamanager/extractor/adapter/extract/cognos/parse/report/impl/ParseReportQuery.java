package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.report.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl.ParseUtil;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class ParseReportQuery {
	private Document reportDoc;
	private Namespace ns;
	private Map<String, List<String>> queryDependncyMap = new HashMap();

	public ParseReportQuery() {
	}

	public Map<String, List<String>> parse(Document reportDoc) {
		this.reportDoc = reportDoc;
		this.ns = reportDoc.getRootElement().getNamespace();

		// 查询所有的查询标签
		List<Element> queryEls = ParseUtil.getChildrenElement(reportDoc,
				"/default:report/default:queries/default:query", ns);

		for (int i = 0; i < queryEls.size(); i++) {
			Element queryEl = queryEls.get(i);

			// 查询的名字
			String queryName = queryEl.getAttributeValue("name");

			Element selectionEl = queryEl.getChild("selection", ns);

			List<Element> dataItemEls = selectionEl.getChildren("dataItem", ns);

			// 解析除了数据项的表达式
			queryEl.removeChild("selection", ns);

			List<String> queryExpressionList = new ArrayList<String>();

			ParseUtil.queryExpressionsInElement(queryEl, queryExpressionList);

			// 数据项中的表达式
			for (int j = 0; j < dataItemEls.size(); j++) {

				Element dataItemEl = dataItemEls.get(j);

				// 数据项名
				String dataItemName = dataItemEl.getAttributeValue("name");

				// 复杂的表达式
				String expressions = dataItemEl.getChildText("expression", ns);

				// 解析出复杂表达式中含有的标准表达式
				List<String> expressionList = ParseUtil
						.selectExpressions(expressions);

				// 数据项外的表达式有依赖关系
				expressionList.addAll(queryExpressionList);

				// 依赖关系
				String[] sl = { queryName, dataItemName };

				queryDependncyMap.put(ParseUtil.buildExpression(sl),
						expressionList);

			}

			// 对于查询自身的引用进行处理
			for (Iterator<String> iterator = queryDependncyMap.keySet()
					.iterator(); iterator.hasNext();) {
				String key = (String) iterator.next();
				List<String> deps = queryDependncyMap.get(key);
				if (deps != null) {
					List<String> addDeps = new ArrayList<String>();
					for (int j = 0; j < deps.size(); j++) {
						String dep = deps.get(j);
						// 自身引用，例如：[*]
						if (dep.indexOf("].[") == -1) {
							List<String> depList = queryDependncyMap.get("["
									+ queryName + "]." + dep);
							if (depList != null) {
								addDeps.addAll(depList);
							}

						}
					}
					deps.addAll(addDeps);
				}

			}

		}

		return queryDependncyMap;
	}

	public Namespace getNs() {
		return ns;
	}

	public void setNs(Namespace ns) {
		this.ns = ns;
	}

	public Document getReportDoc() {
		return reportDoc;
	}

	public void setReportDoc(Document reportDoc) {
		this.reportDoc = reportDoc;
	}

	public Map<String, List<String>> getDdvMap() {
		return queryDependncyMap;
	}

	public void setDdvMap(Map<String, List<String>> ddvMap) {
		this.queryDependncyMap = ddvMap;
	}
}
