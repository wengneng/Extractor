/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.catalogtree.ICatalogElement;
import bof.sdk.ClientConnector;
import bof.sdk.service.catalog.CatalogService;
import bof.sdk.service.simplereport.Field;
import bof.sdk.service.simplereport.Report;
import bof.sdk.service.simplereport.SimpleReportService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * @author 游林杰
 * 
 */
public class SimpleReportParse extends AbstractParse {

	private static Log log = LogFactory.getLog(SimpleReportParse.class);

	/**
	 * 重写解析流程控制
	 * 
	 * @param parent
	 *            父节点
	 * @param element
	 *            接口提供的信息类
	 * @return
	 */
	public BofBO parse(BofBO parent, ICatalogElement element) {
		
		log.debug("enter SimpleReportParse parse");
		Date date1=new Date();

		BofBO bofBO = build(parent, element);

		try {
			ClientConnector conn = getBofConnector().getConnByLimitMethod();
			SimpleReportService simpleReportService = new SimpleReportService(
					conn);
			Report report = simpleReportService.openReportWithoutInit(bofBO.getId());
			List<Field> fields = report.getFields();

			for (int i = 0; i < fields.size(); i++) {
				Field field = fields.get(i);
				ClientConnector conn1 = getBofConnector()
						.getConnByLimitMethod();
				// catalog服务
				CatalogService catalogService = new CatalogService(conn1);
				ICatalogElement fieldElement = catalogService
						.getCatalogElementById(field.getId());
				if (fieldElement != null) {
					// 报表字段的id采用：父结点id+">"+报表字段id+">SimpleReportField>"+序号（从1开始计算）
					BofBO fieldBofBO = new BofBO(bofBO, bofBO.getId() + ">"
							+ fieldElement.getId() + ">SimpleReportField>"
							+ (i + 1));
					fieldBofBO.setCode(fieldElement.getName() + (i + 1));
					fieldBofBO.setName(fieldElement.getName());
					fieldBofBO.setType("REPORT_FIELD");

					// 字段级依赖关系
					fieldBofBO.addDependency(fieldElement.getId());
				}
			}

		} catch (Exception e) {
			log.info("解析Bioffice发生异常,元数据路径：" + bofBO.getContextPath()
					+ ";错误信息：" + e.getMessage());
			AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
					"解析Bioffice发生异常,元数据路径：" + bofBO.getContextPath() + ";错误信息："
							+ e.getMessage());
		}
		
		Date date2=new Date();
		log.debug("耗时:"+(date2.getTime()-date1.getTime())/1000+"秒"); 

		return bofBO;
	}

}
