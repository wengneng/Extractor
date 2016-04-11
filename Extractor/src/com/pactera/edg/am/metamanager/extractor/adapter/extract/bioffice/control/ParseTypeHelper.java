package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.control;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ncr.teradata.jdbc_4.parcel.RewindParcel;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofType;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.BusinessViewFieldParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.BusinessViewParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.DashboardMapParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.DashboardParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.DatasourceParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.FreeReportParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.MetricReportParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.RawSqlBusinessViewParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.SimpleReportParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.impl.TextBusinessViewParse;

public final class ParseTypeHelper {
	private static Map<String, BofType> types = new HashMap<String, BofType>();

	static {
		// 资源库（悬挂点）
		BofType respository = new BofType();
		respository.setName("RESPOSITORY");
		respository.setModelName("BIOffice6_Respository");
		types.put(respository.getName(), respository);

		// 数据源管理
		BofType datasources = new BofType();
		datasources.setName("DATASOURCES");
		datasources.setModelName("BIOffice6_Datasources");
		datasources.setRoot(true);
		types.put(datasources.getName(), datasources);

		// 数据源
		BofType datasource = new BofType();
		datasource.setName("DATASOURCE");
		datasource.setModelName("BIOffice6_Datasource");
		types.put(datasource.getName(), datasource);
		datasource.setParseClass(DatasourceParse.class);

		// 模式
		BofType schema = new BofType();
		schema.setName("SCHEMA");
		schema.setModelName("BIOffice6_Schema");
		types.put(schema.getName(), schema);

		// 基础表
		BofType baseTable = new BofType();
		baseTable.setName("BASETABLE");
		baseTable.setModelName("BIOffice6_BaseTable");
		types.put(baseTable.getName(), baseTable);
		
		// 基础视图
		BofType baseView = new BofType();
		baseView.setName("BASEVIEW");
		baseView.setModelName("BIOffice6_BaseView");
		types.put(baseView.getName(), baseView);
		
		// 字段
		BofType field = new BofType();
		field.setName("FIELD");
		field.setModelName("BIOffice6_Field");
		field.setFieldLevel(true);
		types.put(field.getName(), field);
		
		// 存储过程
		BofType baseProcedure = new BofType();
		baseProcedure.setName("BASEPROCEDURE");
		baseProcedure.setModelName("BIOffice6_Procedure");
		types.put(baseProcedure.getName(), baseProcedure);
		
		// 存储过程字段
		BofType procField = new BofType();
		procField.setName("PROC_FIELD");
		procField.setModelName("BIOffice6_ProcField");
		types.put(procField.getName(), procField);

		// 业务主题目录
		BofType businessThemes = new BofType();
		businessThemes.setName("BUSINESS_THEMES");
		businessThemes.setModelName("BIOffice6_BusinessThemes");
		businessThemes.setRoot(true);
		types.put(businessThemes.getName(), businessThemes);

		// 业务主题
		BofType businessTheme = new BofType();
		businessTheme.setName("BUSINESS_THEME");
		businessTheme.setModelName("BIOffice6_BusinessTheme");
		types.put(businessTheme.getName(), businessTheme);

		// 业务对象
		BofType businessObject = new BofType();
		businessObject.setName("BUSINESS_OBJECT");
		businessObject.setModelName("BIOffice6_BusinessObject");

		List<String> businessObjectDep = new ArrayList<String>();
		businessObjectDep.add("BASETABLE");
		businessObjectDep.add("BASEVIEW");
		businessObject.setDependencys(businessObjectDep);
		types.put(businessObject.getName(), businessObject);

		// 计算字段目录
		BofType calcFields = new BofType();
		calcFields.setName("CALC_FIELDS");
		calcFields.setModelName("BIOffice6_CalcFields");

		types.put(calcFields.getName(), calcFields);

		// 计算字段
		BofType calcField = new BofType();
		calcField.setName("CALC_FIELD");
		calcField.setModelName("BIOffice6_CalcField");
		calcField.setFieldLevel(true);

		List<String> calcFieldDep = new ArrayList<String>();
		calcFieldDep.add("FIELD");
		calcField.setDependencys(calcFieldDep);
		types.put(calcField.getName(), calcField);

		// 业务属性
		BofType businessAttribute = new BofType();
		businessAttribute.setName("BUSINESS_ATTRIBUTE");
		businessAttribute.setModelName("BIOffice6_BusinessAttribute");
		businessAttribute.setFieldLevel(true);

		List<String> businessAttributeDep = new ArrayList<String>();
		businessAttributeDep.add("FIELD");
		businessAttribute.setDependencys(businessAttributeDep);
		types.put(businessAttribute.getName(), businessAttribute);

		// 公有文件夹
		BofType defaultTreenode = new BofType();
		defaultTreenode.setName("DEFAULT_TREENODE");
		defaultTreenode.setModelName("BIOffice6_DefaultTreeNode");
		defaultTreenode.setRoot(true);
		types.put(defaultTreenode.getName(), defaultTreenode);

		// 私有文件夹
		BofType selfTreenode = new BofType();
		selfTreenode.setName("SELF_TREENODE");
		selfTreenode.setModelName("BIOffice6_SelfTreeNode");
		selfTreenode.setRoot(true);
		types.put(selfTreenode.getName(), selfTreenode);

		// 页面
		BofType page = new BofType();
		page.setName("PAGE");
		page.setModelName("BIOffice6_Page");

		List<String> pageDep = new ArrayList<String>();
		pageDep.add("METRIC_REPORT");
		pageDep.add("FREE_REPORT");
		pageDep.add("Dashboard");
		pageDep.add("DashboardMap");
		pageDep.add("SIMPLE_REPORT");
		page.setDependencys(pageDep);
		types.put(page.getName(), page);

		// 公共页面
		BofType publicPage = new BofType();
		publicPage.setName("PUBLIC_PAGES");
		publicPage.setModelName("BIOffice6_PublicPages");
		publicPage.setRoot(true);
		types.put(publicPage.getName(), publicPage);

		// 个人页面
		BofType selfPages = new BofType();
		selfPages.setName("SELF_PAGES");
		selfPages.setModelName("BIOffice6_SelfPages");
		selfPages.setRoot(true);
		types.put(selfPages.getName(), selfPages);

		// 指标报表
		BofType metricReport = new BofType();
		metricReport.setName("METRIC_REPORT");
		metricReport.setModelName("BIOffice6_MetricReport");
		metricReport.setParseClass(MetricReportParse.class);
		
		List<String> metricReportDep = new ArrayList<String>();
		metricReportDep.add("BUSINESS_VIEW");
		metricReportDep.add("TEXT_BUSINESS_VIEW");
		metricReportDep.add("RAWSQL_BUSINESS_VIEW");
		metricReportDep.add("PROC_BUSINESS_VIEW");
		metricReport.setDependencys(metricReportDep);
		types.put(metricReport.getName(), metricReport);

		// 复杂报表
		BofType freeReport = new BofType();
		freeReport.setName("FREE_REPORT");
		freeReport.setModelName("BIOffice6_FreeReport");
		freeReport.setParseClass(FreeReportParse.class);
		
		List<String> freeReportDep = new ArrayList<String>();
		freeReportDep.add("BUSINESS_VIEW");
		freeReportDep.add("TEXT_BUSINESS_VIEW");
		freeReportDep.add("RAWSQL_BUSINESS_VIEW");
		freeReportDep.add("PROC_BUSINESS_VIEW");
		freeReport.setDependencys(freeReportDep);
		types.put(freeReport.getName(), freeReport);

		// 仪表分析
		BofType dashboard = new BofType();
		dashboard.setName("Dashboard");
		dashboard.setModelName("BIOffice6_Dashboard");
		dashboard.setParseClass(DashboardParse.class);
		
		List<String> dashboardDep = new ArrayList<String>();
		dashboardDep.add("BUSINESS_VIEW");
		dashboardDep.add("TEXT_BUSINESS_VIEW");
		dashboardDep.add("RAWSQL_BUSINESS_VIEW");
		dashboardDep.add("PROC_BUSINESS_VIEW");
		dashboard.setDependencys(dashboardDep);
		types.put(dashboard.getName(), dashboard);

		// 地图分析
		BofType dashboardMap = new BofType();
		dashboardMap.setName("DashboardMap");
		dashboardMap.setModelName("BIOffice6_DashboardMap");
		dashboardMap.setParseClass(DashboardMapParse.class);
		
		List<String> dashboardMapDep = new ArrayList<String>();
		dashboardMapDep.add("BUSINESS_VIEW");
		dashboardMapDep.add("TEXT_BUSINESS_VIEW");
		dashboardMapDep.add("RAWSQL_BUSINESS_VIEW");
		dashboardMapDep.add("PROC_BUSINESS_VIEW");
		dashboardMap.setDependencys(dashboardMapDep);
		types.put(dashboardMap.getName(), dashboardMap);

		// 灵活分析
		BofType simpleReport = new BofType();
		simpleReport.setName("SIMPLE_REPORT");
		simpleReport.setModelName("BIOffice6_SimpleReport");
		simpleReport.setParseClass(SimpleReportParse.class);
		
		List<String> simpleReportDep = new ArrayList<String>();
		simpleReportDep.add("BUSINESS_VIEW");
		simpleReportDep.add("TEXT_BUSINESS_VIEW");
		simpleReportDep.add("RAWSQL_BUSINESS_VIEW");
		simpleReportDep.add("PROC_BUSINESS_VIEW");
		simpleReport.setDependencys(simpleReportDep);
		types.put(simpleReport.getName(), simpleReport);

		// 报表字段
		BofType reportField = new BofType();
		reportField.setName("REPORT_FIELD");
		reportField.setModelName("BIOffice6_ReportField");
		reportField.setFieldLevel(true);

		List<String> reportFieldDep = new ArrayList<String>();
		reportFieldDep.add("BUSINESS_VIEW_FIELD");
		reportField.setDependencys(reportFieldDep);
		types.put(reportField.getName(), reportField);

		// 可视化查询
		BofType businessView = new BofType();
		businessView.setName("BUSINESS_VIEW");
		businessView.setModelName("BIOffice6_BusinessView");
		businessView.setParseClass(BusinessViewParse.class);
		types.put(businessView.getName(), businessView);

		// SQL查询
		BofType textBusinessView = new BofType();
		textBusinessView.setName("TEXT_BUSINESS_VIEW");
		textBusinessView.setModelName("BIOffice6_TextBusinessView");
		textBusinessView.setParseClass(TextBusinessViewParse.class);
		types.put(textBusinessView.getName(), textBusinessView);

		// 原生SQL查询
		BofType rawSqlBusinessView = new BofType();
		rawSqlBusinessView.setName("RAWSQL_BUSINESS_VIEW");
		rawSqlBusinessView.setModelName("BIOffice6_RawSqlBusinessView");
		rawSqlBusinessView.setParseClass(RawSqlBusinessViewParse.class);
		types.put(rawSqlBusinessView.getName(), rawSqlBusinessView);

		// 查询输出字段
		BofType businessViewField = new BofType();
		businessViewField.setName("BUSINESS_VIEW_FIELD");
		businessViewField.setModelName("BIOffice6_BusinessViewField");
		businessViewField.setParseClass(BusinessViewFieldParse.class);
		businessViewField.setFieldLevel(true);

		List<String> businessViewFieldDep = new ArrayList<String>();
		businessViewFieldDep.add("CALC_FIELDS");
		businessViewFieldDep.add("BUSINESS_ATTRIBUTE");
		businessViewFieldDep.add("FIELD");
		businessViewFieldDep.add("PROC_FIELD");
		businessViewField.setDependencys(businessViewFieldDep);
		types.put(businessViewField.getName(), businessViewField);
		
		// 存储过程查询
		BofType procBusinessView = new BofType();
		procBusinessView.setName("PROC_BUSINESS_VIEW");
		procBusinessView.setModelName("BIOffice6_ProcBusinessView");
		procBusinessView.setParseClass(RawSqlBusinessViewParse.class);
		
		List<String> procBusinessViewDep = new ArrayList<String>();
		procBusinessViewDep.add("BASEPROCEDURE");
		procBusinessView.setDependencys(procBusinessViewDep);
		types.put(procBusinessView.getName(), procBusinessView);

	}

	public static boolean isRoot(String typeName) {
		BofType type = types.get(typeName);
		if (type != null && type.isRoot()) {
			return true;
		}
		return false;
	}

	/**
	 * 判断该类型是否在解析范围内
	 * 
	 * @param type
	 * @return
	 */
	public static boolean existInstanceType(String typeName) {
		return types.get(typeName) != null;
	}

	/**
	 * 判断当然被依赖类型是否存在
	 * 
	 * @param type
	 * @return
	 */
	public static boolean existDeped(String typeName, String depedTypeName) {
		List deps = types.get(typeName).getDependencys();
		for (int i = 0; i < deps.size(); i++) {
			if (depedTypeName.equals(deps.get(i))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 获取类型对象
	 * 
	 * @param typeName
	 * @return
	 */
	public static BofType getBofType(String typeName) {
		return types.get(typeName);
	}

	/**
	 * 根据类型判断是否需要解析依赖关系
	 * 
	 * @param typeName
	 * @return
	 */
	public static boolean hasDependency(String typeName) {
		List<String> dependencys = types.get(typeName).getDependencys();
		if (dependencys == null || dependencys.size() == 0) {
			return false;
		}
		return true;
	}

}
