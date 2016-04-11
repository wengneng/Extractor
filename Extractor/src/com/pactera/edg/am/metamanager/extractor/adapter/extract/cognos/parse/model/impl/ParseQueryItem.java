package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Namespace;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;

public class ParseQueryItem extends AModel {
	private Log log = LogFactory.getLog(ParseModel.class);

	public ParseQueryItem(Document xmlDoc, String defaultLocal, Namespace ns) {
		this.xmlDoc = xmlDoc;
		this.defaultLocal = defaultLocal;
		this.ns = ns;
	}

	public void parseQueryItem(CognosBO cb) {
		CognosBO querySubjectBO = cb.getParent();
		if (querySubjectBO.getDependency().size() > 0) {
			CognosBO depTableBO = querySubjectBO.getDependency().get(0);
			if ("columnSet".equals(depTableBO.getType())
					|| "storedProcedure".equals(depTableBO.getType())) {
				String columnName = cb.getCode().toUpperCase();
				CognosBO columnBO = depTableBO.queryChildrenByCodeAndType(
						columnName, "column");
				if (columnBO == null) {
					columnBO = new CognosBO(depTableBO);
					columnBO.setCode(columnName);
					columnBO.setName(columnName);
					columnBO.setType("column");
					depTableBO.getChildren().add(columnBO);
				}
				cb.getDependency().add(columnBO);
			}
		}
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}
}
