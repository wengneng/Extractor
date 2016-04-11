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
import bof.sdk.service.portal.ChartDefine;
import bof.sdk.service.portal.ChartField;
import bof.sdk.service.portal.DashboardService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * @author yinghua_kang
 * 
 */
public class DashboardParse extends AbstractParse {

	private static Log log = LogFactory.getLog(DashboardParse.class);

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
		log.debug("enter DashboardParse parse");
		Date date1=new Date();
		
		BofBO bofBO = super.build(parent, element);

		try {
			ClientConnector conn = getBofConnector().getConnByLimitMethod();
			// Dashboard服务
			DashboardService dashboardService = new DashboardService(conn);

			dashboardService.getDashboard(bofBO.getId());
			ChartDefine chartDefine = dashboardService.getChartDefine();

			List<ChartField> fields = chartDefine.getChartFields();

			for (int i = 0; i < fields.size(); i++) {
				ChartField field = fields.get(i);
				ClientConnector conn1 = getBofConnector()
						.getConnByLimitMethod();
				// catalog服务
				CatalogService catalogService = new CatalogService(conn1);

				ICatalogElement fieldElement = catalogService
						.getCatalogElementById(field.getId());
				if (fieldElement != null) {
					// 报表字段的id采用：父结点id+">"+报表字段id+">DashboardField>"+序号（从1开始计算）
					BofBO fieldBofBO = new BofBO(bofBO, bofBO.getId() + ">"
							+ fieldElement.getId() + ">DashboardField>"
							+ (i + 1));

					fieldBofBO = bulidBaseInfoFromElement(fieldBofBO,
							fieldElement.getName() + (i + 1), fieldElement
									.getAlias(), "REPORT_FIELD");

					// 字段级依赖关系
					fieldBofBO.addDependency(fieldElement.getId());
				}
			}

		} catch (Exception e) {
			log.debug("解析Bioffice发生异常,元数据路径：" + bofBO.getContextPath()
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
