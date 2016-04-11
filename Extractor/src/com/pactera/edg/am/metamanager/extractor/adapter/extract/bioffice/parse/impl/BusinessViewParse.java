package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.sdk.ClientConnector;
import bof.sdk.service.businessview.BusinessViewService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

public class BusinessViewParse extends AbstractParse {

	private static Log log = LogFactory.getLog(BusinessViewParse.class);

	/**
	 * 重写获取可视化查询的SQL属性
	 */
	protected void buildAttribute(BofBO bofBO) {
		ClientConnector conn = null;
		try {
			conn=getBofConnector().getConnByLimitMethod();
			BusinessViewService businessViewService=new BusinessViewService(conn);	
			String sql = businessViewService.getSqlString(bofBO.getId());
			
			// 可视化查询的SQL属性
			bofBO.getAttributes().put("sql", sql);
		} catch (Exception e) {
			log.debug("解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
			AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
					"解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
		}

	}

}
