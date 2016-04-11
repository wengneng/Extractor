package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.report.impl;

import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Element;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl.ParseUtil;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class ParseReportFile {
	private Document reportDoc;
	private CognosBO cb;

	/**
	 * @return ReportFileVO
	 * @roseuid 4A6415E40148
	 */
	public CognosBO parse(CognosBO cb, Document reportDoc) {
		this.reportDoc = reportDoc;
		this.cb = cb;

		System.out.println("search path:" + cb.getSearchPath());
//		ParseUtil.printXml(reportDoc);

		// 报表用到的模型
		Element modelPathEl = reportDoc.getRootElement().getChild("modelPath",
				reportDoc.getRootElement().getNamespace());
		if (modelPathEl != null) {
			cb.getAttributes().put("modelPath", modelPathEl.getValue());
		}

		// 解析报表文件的查询
		ParseReportQuery prq = new ParseReportQuery();

		Map<String, List<String>> queryDependncyMap = prq.parse(reportDoc);

		// 解析报表文件的报表
		ParseReportLayOut pr = new ParseReportLayOut();

		cb.setChildren(pr.parse(reportDoc, queryDependncyMap, cb));

		return cb;
	}

	public Document getReportDoc() {
		return reportDoc;
	}

	public void setReportDoc(Document reportDoc) {
		this.reportDoc = reportDoc;
	}
}
