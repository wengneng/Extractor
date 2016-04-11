package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlThirdAdapter;

public class PowerCenterXsltTransformer extends AbstractXmlThirdAdapter {

	@Override
	protected String getInValidType() {
		return "DTD";
	}

	@Override
	protected String getInXsdOrDtd() {
		return "classpath:/extractor/adapter/xsl/powrmart.dtd";
	}

	@Override
	protected String getToolName() {
		return "powerCenter";
	}

	@Override
	protected String getVersion() {
		return "8/6";
	}

	@Override
	protected String getXslt() {
		return "classpath:/extractor/adapter/xsl/powerCenter.xsl";
	}

	@Override
	protected void clear() {
		
	}
	
	
}
