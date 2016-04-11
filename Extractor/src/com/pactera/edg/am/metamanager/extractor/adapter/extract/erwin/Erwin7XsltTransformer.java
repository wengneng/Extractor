package com.pactera.edg.am.metamanager.extractor.adapter.extract.erwin;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlThirdAdapter;

public class Erwin7XsltTransformer extends AbstractXmlThirdAdapter {

	@Override
	protected String getInValidType() {
		return null;
	}

	@Override
	protected String getInXsdOrDtd() {
		return null;
	}

	@Override
	protected String getToolName() {
		return "erwin";
	}

	@Override
	protected String getVersion() {
		return "7.*";
	}

	@Override
	protected String getXslt() {
		return "classpath:/extractor/adapter/xsl/erwin7.xsl";
	}

	@Override
	protected void clear() {

	}

}
