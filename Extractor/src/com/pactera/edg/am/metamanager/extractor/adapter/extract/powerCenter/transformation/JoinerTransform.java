package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation;

import java.util.Set;

import org.jdom.Element;

public class JoinerTransform extends AbstractTransform {
	/*demo
	 * <TRANSFORMATION DESCRIPTION ="" NAME ="JNRTRANS" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Joiner" VERSIONNUMBER ="3">
		    <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="FOLDER_NAME" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT/MASTER" PRECISION ="50" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="WORKFLOW_NAME" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT/MASTER" PRECISION ="50" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="SESSION_NAME" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT/MASTER" PRECISION ="150" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="PARAMETER_NAME" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT/MASTER" PRECISION ="50" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="double" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="PARAMETER_TYPE" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT/MASTER" PRECISION ="15" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="PARAMETER_VALUE" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT/MASTER" PRECISION ="150" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="MAPPLET_NAME" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT/MASTER" PRECISION ="50" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="ID_Tbl" PICTURETEXT ="" PORTTYPE ="INPUT/MASTER" PRECISION ="10" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="DATADATE" PICTURETEXT ="" PORTTYPE ="INPUT/OUTPUT" PRECISION ="19" SCALE ="0"/>
		    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" DESCRIPTION ="" NAME ="ID_FIL" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="10" SCALE ="0"/>
		    <TABLEATTRIBUTE NAME ="Case Sensitive String Comparison" VALUE ="YES"/>
		    <TABLEATTRIBUTE NAME ="Cache Directory" VALUE ="$PMCacheDir"/>
		    <TABLEATTRIBUTE NAME ="Join Condition" VALUE ="ID_Tbl = ID_FIL"/>
		    <TABLEATTRIBUTE NAME ="Join Type" VALUE ="Normal Join"/>
		    <TABLEATTRIBUTE NAME ="Null ordering in master" VALUE ="Null Is Highest Value"/>
		    <TABLEATTRIBUTE NAME ="Null ordering in detail" VALUE ="Null Is Highest Value"/>
		    <TABLEATTRIBUTE NAME ="Tracing Level" VALUE ="Normal"/>
		    <TABLEATTRIBUTE NAME ="Joiner Data Cache Size" VALUE ="Auto"/>
		    <TABLEATTRIBUTE NAME ="Joiner Index Cache Size" VALUE ="Auto"/>
		    <TABLEATTRIBUTE NAME ="Sorted Input" VALUE ="NO"/>
		    <TABLEATTRIBUTE NAME ="Master Sort Order" VALUE ="Auto"/>
		    <TABLEATTRIBUTE NAME ="Transformation Scope" VALUE ="All Input"/>
        </TRANSFORMATION>
	 */
	// 重写获取output的方法
	protected void getDepOutput(String fieldName, Element fieldlement,
			Set<String> depFieldList) {
		String type=fieldlement.getAttributeValue("PORTTYPE");
		if(type.startsWith("INPUT/OUTPUT")){
			depFieldList.add(fieldName);
		}
	}
}


