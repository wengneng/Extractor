package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.sdk.ClientConnector;
import bof.sdk.service.businessview.BusinessViewService;
import bof.sdk.service.metadata.DocumentTreeNode;
import bof.sdk.service.metadata.MetadataService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;


public class TextBusinessViewParse extends AbstractParse {

	private static Log log = LogFactory.getLog(TextBusinessViewParse.class);

	/**
	 * SQL查询
	 */
	protected void buildAttribute(BofBO bofBO) {
		try {
			ClientConnector conn = getBofConnector().getConnByLimitMethod();
			BusinessViewService businessViewService = new BusinessViewService(conn);

			String sql = businessViewService.getSqlString(bofBO.getId());

			//对^符号替换为'
			sql=sql.replace("^", "'");
			sql=StringUtils.remove(sql, "\""); 
			// SQL属性
			bofBO.getAttributes().put("sql", sql);

			// 查询出关联的数据源ID
			String datasourceId = null;

			ClientConnector conn1 = getBofConnector().getConnByLimitMethod();
			MetadataService metadataService = new MetadataService(conn1);

			List<DocumentTreeNode> documentTreeNodes = metadataService
					.searchReferringTo(bofBO.getId(), false);
			
			for (int i = 0; i < documentTreeNodes.size(); i++) {
				// 存在依赖关系并且是字段级的
				if ("DATASOURCE".equals(documentTreeNodes.get(i).getType())) {
					datasourceId = documentTreeNodes.get(i).getId();
					bofBO.getAttributes().put("datasourceId", datasourceId);
				}
			}
		} catch (Exception e) {
			log.debug("解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
			AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
					"解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
		}

	}
}
