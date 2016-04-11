package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlAdapter;

/**
 * Excel的xml格式转换适配器
 *
 * @author wangyang
 * @version 1.0  Date: Sep 25, 2009
 */
public class ExcelXmlAdapter extends AbstractXmlAdapter {
	private final String TOOLNAME = "MM";
	private final String VERSION = "1.0";
	@Override
	protected String getInValidType() {
		return null;
	}

	@Override
	protected String getInXsdOrDtd() {
		return null;
	}
	
	@Override
	protected final String getOutXsd(String templetId){
		return null;
	}
	
	@Override
	protected final String getOutXsd() {
		return null;
	}

	@Override
	protected String getToolName() {
		return this.TOOLNAME;
	}

	@Override
	protected String getVersion() {
		return VERSION;
	}

	@Override
	protected String getXslt() {
		return "classpath:/extractor/adapter/xsl/excel.xslt";
	}

	@Override
	protected boolean isThirdParty() {
		return false;
	}

	@Override
	protected void clear() {
		
	}
}
