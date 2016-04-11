package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.test;

import java.util.ArrayList;
import java.util.List;

import bof.catalogtree.ICatalogElement;
import bof.managereport.DataSetFieldInfo;
import bof.managereport.MetricInfo;
import bof.net.sf.json.JSONArray;
import bof.sdk.ClientConnector;
import bof.sdk.service.catalog.CatalogService;
import bof.sdk.service.managereport.ComplexReport;
import bof.sdk.service.managereport.ManageReportService;
import bof.sdk.service.managereport.MetricReport;
import bof.sdk.service.metadata.DocumentTreeNode;
import bof.sdk.service.metadata.MetadataService;
import bof.sdk.service.portal.ChartDefine;
import bof.sdk.service.portal.ChartField;
import bof.sdk.service.portal.DashboardService;
import bof.sdk.service.simplereport.Report;
import bof.sdk.service.simplereport.SimpleReportService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.webService.BofConnector;

public class Test {
	public static void main(String[] args) {
		BofConnector bct=new BofConnector();
		bct.setIp("127.0.0.1");
		bct.setUserName("admin");
		bct.setPassword("manager");
		bct.setPort("8080");
		
		connectTest(bct);
		
//		analysisTest(bct);
		
//		reportTest(bct);
		
		
//		searchByReferencedRecursive(bct);
	}
	
	/**
	 * 连接测试
	 * @param bct
	 */
	private static void connectTest(BofConnector bct){
		
		SimpleReportService simpleReportService = new SimpleReportService(
				bct.getConnByLimitMethod());
		Report report = simpleReportService.openReportWithoutInit("I2c949eaf1a942102011a9561f7e7015d");
		System.out.println(report.getFields());
//		Long date1 = System.currentTimeMillis();
//		for (int i = 0; i < 2000; i++) {		
//			ClientConnector cct= bct.getConnByLimitMethod();
//			CatalogService catalogService=new CatalogService(cct);
//			ICatalogElement el=catalogService.getCatalogElementById("PROCFIELD.KPI.edwBVIEW..STA_DWN_H_ACCT_CNT_MC.start_date");
//			List<ICatalogElement> children = catalogService
//			.getChildElements("I40288fab2af3fbf9012af53185f70c72");
//			int a=1;
//		}
//		bct.closeConn();
//		Long date2=System.currentTimeMillis();
//		System.out.println("每次查询都新建连接，查询2000次子节点。共耗时："+(date2-date1)/1000+"秒"); 
//		
//		
//		Long date3 = System.currentTimeMillis();
//		ClientConnector cct1= bct.getConnByLimitMethod();
//		for (int i = 0; i < 2000; i++) {
//			CatalogService catalogService1=new CatalogService(cct1);
//			List<ICatalogElement> children = catalogService1
//			.getChildElements("I2c90903e114ef1af01114f1a533f007b");
//		}
//		cct1.close();
//		
//		Long date4=System.currentTimeMillis();
//		System.out.println("只建立一次连接，查询2000次子节点。共耗时："+(date4-date3)/1000+"秒"); 
	}
	
	/**
	 * 分析测试
	 * @param bct
	 */
	private static void analysisTest(BofConnector bct){
		Long date1 = System.currentTimeMillis();
		for (int i = 0; i < 200; i++) {
			ClientConnector cct= bct.getConnByLimitMethod();
			MetadataService metadataService = new MetadataService(cct);
			// 依赖关系分析			
			List<DocumentTreeNode> documentTreeNodes = metadataService.searchReferringTo("I2c90905413dd56e40113dd676a810066", false);
			cct.close();
		}
		Long date2=System.currentTimeMillis();
		System.out.println("每次都建立连接，分析200次。共耗时："+(date2-date1)/1000+"秒"); 
		
		Long date3 = System.currentTimeMillis();
		ClientConnector cct1= bct.getConnByLimitMethod();
		MetadataService metadataService1 = new MetadataService(cct1);
		for (int i = 0; i < 2000; i++) {		
			// 依赖关系分析			
			List<DocumentTreeNode> documentTreeNodes = metadataService1.searchReferringTo("I2c90905413dd56e40113dd676a810066", false);
			int j=0;
		}
		cct1.close();
		
		Long date4=System.currentTimeMillis();
		System.out.println("只建立一次连接，分析2000次。共耗时："+(date4-date3)/1000+"秒"); 
		
		
//		Long date5 = System.currentTimeMillis();
//		ClientConnector cct2= bct.getConnByLimitMethodector();
//		bof.metadata.assist.CategoryResource res = new bof.metadata.assist.CategoryResource();
//		res.setId("I2c90905413dd56e40113dd676a810066");
//		List<String> filters = new ArrayList<String>();
//		boolean recursive = false;
		
//		for (int i = 0; i < 200; i++) {
//			bof.sdk.InvokeResult result = cct2.remoteInvoke("MetadataService",
//					"searchByReferencedRecursive", new Object[] { res, filters, recursive });
//					JSONArray jsonObject = (JSONArray) result.getResult();
//		}
//		cct2.close();
//		
//		Long date6 = System.currentTimeMillis();
//		System.out.println("只建立一次连接，IMetadataModule方法分析200次。共耗时："+(date6-date5)/1000+"秒"); 
		
	}
	
	/**
	 * 报表测试
	 * @param bct
	 */
	private static void reportTest(BofConnector bct){
		Long date1 = System.currentTimeMillis();
		for (int i = 0; i < 200; i++) {	
			ClientConnector cct= bct.getConnByLimitMethod();
			DashboardService dashboardService = new DashboardService(cct);
			dashboardService.getDashboard("I2c928da527311aeb012732a5ac750bc8");
			ChartDefine chartDefine = dashboardService.getChartDefine();
			List<ChartField> fields = chartDefine.getChartFields();
			int j=0;
		}
		bct.closeConn();
		
		Long date2=System.currentTimeMillis();
		System.out.println("连接调用次数限制，打开仪表查询200次。共耗时："+(date2-date1)/1000+"秒"); 
		
//		Long date3 = System.currentTimeMillis();
//		for (int i = 0; i < 200; i++) {	
//			ClientConnector cct1= bct.getConnByLimitMethod();
//			ManageReportService manageReportService=new ManageReportService(cct1);
//			ComplexReport report = manageReportService.openComplexReportByName("jjh");
//			List<DataSetFieldInfo> fields = report.getDatasetFields();
//			int j=0;
//		}
//		bct.closeConn();
//		
//		Long date4=System.currentTimeMillis();
//		System.out.println("只建立一次连接，打开复杂报表200次。共耗时："+(date4-date3)/1000+"秒"); 
		
		
	}
	
	
	/**
	 * 连接测试
	 * @param bct
	 */
	private static void searchByReferencedRecursive(BofConnector bct){
		Long date1 = System.currentTimeMillis();
		for (int i = 0; i < 2000; i++) {		
			ClientConnector connector= bct.getConnByLimitMethod();
			
			bof.metadata.assist.CategoryResource res = new bof.metadata.assist.CategoryResource();
			res.setId("I2c908b4b23839107012383e2f7b8006b");
//			List<String> filters = new ArrayList<String>();
			boolean recursive = false;
			bof.sdk.InvokeResult result = connector.remoteInvoke("MetadataService",
					"searchReferringToRecursive", new Object[] { res, null, recursive });

			JSONArray jsonObject = (JSONArray) result.getResult();
			
//			for (int j = 0; j < jsonObject.length(); j++) {
//				System.out.println("id:"+jsonObject.getJSONObject(j).getString("id"));
//			}
 
			int o=0;
			
		}
		bct.closeConn();
		Long date2=System.currentTimeMillis();
		System.out.println("每次查询都新建连接，查询2000次子节点。共耗时："+(date2-date1)/1000+"秒"); 
		
		
		Long date3 = System.currentTimeMillis();
		
		for (int i = 0; i < 2000; i++) {	
			ClientConnector connector= bct.getConnByLimitMethod();
			MetadataService metadataService1 = new MetadataService(connector);
			// 依赖关系分析			
			List<DocumentTreeNode> documentTreeNodes = metadataService1.searchReferringTo("I2c908b4b23839107012383e2f7b8006b", false);
			int j=0;
		}
		bct.closeConn();
		
		Long date4=System.currentTimeMillis();
		System.out.println("只建立一次连接，分析2000次。共耗时："+(date4-date3)/1000+"秒"); 
		

	}
}
