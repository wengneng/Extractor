package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation;

import java.util.Set;

import org.jdom.Element;

public class OutputTransform extends AbstractTransform {
	/*
	 * <TRANSFORMATION DESCRIPTION ="" NAME ="OUTPUT" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Output Transformation" VERSIONNUMBER ="1">
     *      <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="inputFiled1" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="10" SCALE ="0"/>
     *  </TRANSFORMATION>
	 */
	// 重写获取output的方法
	protected void getDepOutput(String fieldName, Element fieldlement,
			Set<String> depFieldList) {
		depFieldList.add(fieldName);
	}
}
