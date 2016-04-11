package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl;

import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Namespace;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;

public class AModel {
	protected Document xmlDoc;
	protected String defaultLocal;
	protected Namespace ns;

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
}
