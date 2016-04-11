/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.catalogtree.ICatalogElement;
import bof.managereport.MetricInfo;
import bof.sdk.ClientConnector;
import bof.sdk.service.managereport.ManageReportService;
import bof.sdk.service.managereport.MetricReport;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * @author yinghua_kang
 * 
 */
public class MetricReportParse extends AbstractParse {
	private Log log = LogFactory.getLog(MetricReportParse.class);

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
		
		log.debug("enter MetricReportParse parse");
		Date date1=new Date();

		BofBO bofBO = build(parent, element);
		
		ClientConnector conn = null;
		try {
			conn = getBofConnector().getConnByLimitMethod();
			ManageReportService manageReportService=new ManageReportService(conn);
			MetricReport report = manageReportService.openMetricReportByName(bofBO.getCode());

			if (report == null) {
				log.debug("没有取到Code为" + bofBO.getCode() + "的指标报表");
				return bofBO;
			}
			List<MetricInfo> fields = report.getMetrics();

			for (int i = 0; i < fields.size(); i++) {
				MetricInfo fieldElement = fields.get(i);
				if (fieldElement != null) {
					// 报表字段的id采用：父结点id+">"+报表字段id+">MetricReportField>"+序号（从1开始计算）
					BofBO fieldBofBO = new BofBO(bofBO, bofBO.getId() + ">" + fieldElement.getId() + ">MetricReportField>"
							+ (i + 1));
					fieldBofBO = bulidBaseInfoFromElement(fieldBofBO, fieldElement.getName() + (i + 1), fieldElement
							.getAlias(), "REPORT_FIELD");

					// 字段级依赖关系
					fieldBofBO.addDependency(fieldElement.getId());
				}
			}
		} catch (Exception e) {
			log.debug("解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
			AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
					"解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
		}

		Date date2=new Date();
		log.debug("耗时:"+(date2.getTime()-date1.getTime())/1000+"秒"); 


		return bofBO;
	}
}
