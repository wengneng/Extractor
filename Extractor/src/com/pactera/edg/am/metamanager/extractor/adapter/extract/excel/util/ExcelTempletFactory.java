package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel.util;

import java.io.File;

/**
 * Excel模板工厂类
 * @author wanglei 2009-09-17
 */
public class ExcelTempletFactory extends XmlAbstractFactory {
	private final String xslt = "excelTemplet.xsl";
	private final String xsd = "excelTemplet.xsd";
	private final String confPath = "templet";
	
	
	public String getXsl() throws Exception {
		return confPath +File.separator+ xslt;
	}
	
	public String getXsd() throws Exception {
		return confPath +File.separator+ xsd;
	}
	
	boolean parserSource(String xml) throws Exception{
		return true;
	}

}
