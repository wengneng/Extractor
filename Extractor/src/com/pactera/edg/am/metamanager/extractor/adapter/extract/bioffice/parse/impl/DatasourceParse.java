package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.sdk.ClientConnector;
import bof.sdk.service.oltpmetadata.DataSource;
import bof.sdk.service.oltpmetadata.OltpMetadataService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

public class DatasourceParse extends AbstractParse {
	
	private static Log log = LogFactory.getLog(DashboardParse.class);

	/**
	 * 重写获取数据源属性
	 */
	protected void buildAttribute(BofBO bofBO) {
	
		ClientConnector conn = null;
		try {
			conn = getBofConnector().getConnByLimitMethod();
			OltpMetadataService oltpMetadataService = new OltpMetadataService(conn);
			DataSource ds = oltpMetadataService.getDataSource(bofBO.getId());
			// 连接字符串信息
			bofBO.getAttributes().put("url", ds.getUrl());
			bofBO.getAttributes().put("driver", ds.getDriver());
			bofBO.getAttributes().put("driverType", ds.getDriverType().name());
			bofBO.getAttributes().put("user", ds.getUser());
		} catch (Exception e) {
			log.debug("解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
			AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
					"解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
		}

	}
}
