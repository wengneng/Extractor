/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.sdk.ClientConnector;
import bof.sdk.service.metadata.DocumentTreeNode;
import bof.sdk.service.metadata.MetadataService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.control.ParseTypeHelper;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * @author yinghua_kang
 * 
 */
public class BusinessViewFieldParse extends AbstractParse {
	
	private static Log log = LogFactory.getLog(BusinessViewFieldParse.class);

	/**
	 * 重写创建依赖关系方法
	 * 
	 * @param bofBO
	 * @param documentTreeNode
	 */
	protected void buildDependency(BofBO bofBO) {
		this.buildDependencyByParent(bofBO);
	}
	
	
}
