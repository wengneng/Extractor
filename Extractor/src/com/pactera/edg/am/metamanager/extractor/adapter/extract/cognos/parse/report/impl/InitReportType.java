package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.report.impl;

import java.util.ArrayList;
import java.util.List;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.AttributeBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.ReportTypeBO;

public class InitReportType {
	private List<ReportTypeBO> reportTypes = new ArrayList<ReportTypeBO>();

	// 初始化报表类型数据
	public InitReportType() {

		// 列表
		ReportTypeBO list = new ReportTypeBO();
		list.setXmlLabel("list");
		list.setName("列表");

		AttributeBO listAttr1 = new AttributeBO();
		listAttr1.setName("列表项");
		listAttr1.getXmlLabelScope().add("listColumns");
		listAttr1.getXmlLabelScope().add("listColumn");

		list.getAttributes().add(listAttr1);
		reportTypes.add(list);

		// 交叉表
		ReportTypeBO crosstab = new ReportTypeBO();
		crosstab.setXmlLabel("crosstab");
		crosstab.setName("交叉表");

		AttributeBO crosstabAttr1 = new AttributeBO();
		crosstabAttr1.setName("交叉行");
		crosstabAttr1.getXmlLabelScope().add("crosstabRows");
		crosstabAttr1.getXmlLabelScope().add("crosstabNodeMember");

		AttributeBO crosstabAttr2 = new AttributeBO();
		crosstabAttr2.setName("交叉列");
		crosstabAttr2.getXmlLabelScope().add("crosstabColumns");
		crosstabAttr2.getXmlLabelScope().add("crosstabNodeMember");

		AttributeBO crosstabAttr3 = new AttributeBO();
		crosstabAttr3.setName("交叉角");
		crosstabAttr3.getXmlLabelScope().add("crosstabCorner");

		AttributeBO crosstabAttr4 = new AttributeBO();
		crosstabAttr4.setName("度量");
		crosstabAttr4.getXmlLabelScope().add("defaultMeasure");

		crosstab.getAttributes().add(crosstabAttr1);
		crosstab.getAttributes().add(crosstabAttr2);
		crosstab.getAttributes().add(crosstabAttr3);
		crosstab.getAttributes().add(crosstabAttr4);
		reportTypes.add(crosstab);

		// 地图
		ReportTypeBO mapChart = new ReportTypeBO();
		mapChart.setXmlLabel("mapChart");
		mapChart.setName("地图");

		AttributeBO mapChartAttr1 = new AttributeBO();
		mapChartAttr1.setName("区域层颜色");
		mapChartAttr1.getXmlLabelScope().add("regionMeasure");

		AttributeBO mapChartAttr2 = new AttributeBO();
		mapChartAttr2.setName("区域层位置");
		mapChartAttr2.getXmlLabelScope().add("mapRegionLocation");

		AttributeBO mapChartAttr3 = new AttributeBO();
		mapChartAttr3.setName("区域层细化位置");
		mapChartAttr3.getXmlLabelScope().add("mapRegionRefinementLocation");

		AttributeBO mapChartAttr4 = new AttributeBO();
		mapChartAttr4.setName("点层颜色");
		mapChartAttr4.getXmlLabelScope().add("pointMeasure");

		AttributeBO mapChartAttr5 = new AttributeBO();
		mapChartAttr5.setName("点层大小");
		mapChartAttr5.getXmlLabelScope().add("pointSizeMeasure");

		AttributeBO mapChartAttr6 = new AttributeBO();
		mapChartAttr6.setName("点层位置");
		mapChartAttr6.getXmlLabelScope().add("mapPointLocation");

		AttributeBO mapChartAttr7 = new AttributeBO();
		mapChartAttr7.setName("点层细化位置");
		mapChartAttr7.getXmlLabelScope().add("mapPointRefinementLocation");

		mapChart.getAttributes().add(mapChartAttr1);
		mapChart.getAttributes().add(mapChartAttr2);
		mapChart.getAttributes().add(mapChartAttr3);
		mapChart.getAttributes().add(mapChartAttr4);
		mapChart.getAttributes().add(mapChartAttr5);
		mapChart.getAttributes().add(mapChartAttr6);
		mapChart.getAttributes().add(mapChartAttr7);
		reportTypes.add(mapChart);

		// 重复器
		ReportTypeBO repeater = new ReportTypeBO();
		repeater.setXmlLabel("repeater");
		repeater.setName("重复器");

		AttributeBO repeaterAttr1 = new AttributeBO();
		repeaterAttr1.setName("重复项");

		repeater.getAttributes().add(repeaterAttr1);
		reportTypes.add(repeater);

		// 图表
		//饼状图
		ReportTypeBO pieChart = new ReportTypeBO();
		pieChart.setXmlLabel("pieChart");
		pieChart.setName("饼状图");
		
		AttributeBO pieChartAttr1 = new AttributeBO();
		pieChartAttr1.setName("饼形图");
		pieChartAttr1.getXmlLabelScope().add("pies");
		
		AttributeBO pieChartAttr2 = new AttributeBO();
		pieChartAttr2.setName("饼形图片区");
		pieChartAttr2.getXmlLabelScope().add("piesSlices");
		
		AttributeBO pieChartAttr3 = new AttributeBO();
		pieChartAttr3.setName("度量");
		pieChartAttr3.getXmlLabelScope().add("defaultChartMeasure");
		
		pieChart.getAttributes().add(pieChartAttr1);
		pieChart.getAttributes().add(pieChartAttr2);
		pieChart.getAttributes().add(pieChartAttr3);
		
		reportTypes.add(pieChart);
		
		
		//排列图
		ReportTypeBO paretoChart = new ReportTypeBO();
		paretoChart.setXmlLabel("paretoChart");
		paretoChart.setName("排列图");
		
		AttributeBO paretoChartAttr1 = new AttributeBO();
		paretoChartAttr1.setName("度量");
		paretoChartAttr1.getXmlLabelScope().add("defaultChartMeasure");
		
		AttributeBO paretoChartAttr2 = new AttributeBO();
		paretoChartAttr2.setName("系列");
		paretoChartAttr2.getXmlLabelScope().add("paretoBars");
		
		AttributeBO paretoChartAttr3 = new AttributeBO();
		paretoChartAttr3.setName("类别");
		paretoChartAttr3.getXmlLabelScope().add("paretoBarClusters");
		
		paretoChart.getAttributes().add(paretoChartAttr1);
		paretoChart.getAttributes().add(paretoChartAttr2);
		paretoChart.getAttributes().add(paretoChartAttr3);
		
		reportTypes.add(paretoChart);
		
		
		
		

		ReportTypeBO progressiveChart = new ReportTypeBO();
		progressiveChart.setXmlLabel("progressiveChart");
		progressiveChart.setName("progressiveChart");
		reportTypes.add(progressiveChart);

		ReportTypeBO combinationChart = new ReportTypeBO();
		combinationChart.setXmlLabel("combinationChart");
		combinationChart.setName("combinationChart");
		reportTypes.add(combinationChart);

		ReportTypeBO scatterChart = new ReportTypeBO();
		scatterChart.setXmlLabel("scatterChart");
		scatterChart.setName("scatterChart");
		reportTypes.add(scatterChart);

		ReportTypeBO bubbleChart = new ReportTypeBO();
		bubbleChart.setXmlLabel("bubbleChart");
		bubbleChart.setName("bubbleChart");
		reportTypes.add(bubbleChart);

		ReportTypeBO threeDScatterChart = new ReportTypeBO();
		threeDScatterChart.setXmlLabel("threeDScatterChart");
		threeDScatterChart.setName("threeDScatterChart");
		reportTypes.add(threeDScatterChart);

		ReportTypeBO radarChart = new ReportTypeBO();
		radarChart.setXmlLabel("radarChart");
		radarChart.setName("radarChart");
		reportTypes.add(radarChart);

		ReportTypeBO polarChart = new ReportTypeBO();
		polarChart.setXmlLabel("polarChart");
		polarChart.setName("polarChart");
		reportTypes.add(polarChart);

		ReportTypeBO gaugeChart = new ReportTypeBO();
		gaugeChart.setXmlLabel("gaugeChart");
		gaugeChart.setName("gaugeChart");
		reportTypes.add(gaugeChart);

		ReportTypeBO metricsChart = new ReportTypeBO();
		metricsChart.setXmlLabel("metricsChart");
		metricsChart.setName("metricsChart");
		reportTypes.add(metricsChart);

	}

	public List<ReportTypeBO> getReportTypes() {
		return reportTypes;
	}

	public void setReportTypes(List<ReportTypeBO> reportTypes) {
		this.reportTypes = reportTypes;
	}
}
