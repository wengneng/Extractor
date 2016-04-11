package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcUtils;

public class UnionTransform extends AbstractTransform {
   /*demo
	*  <TRANSFORMATION COMPONENTVERSION ="1000000" DESCRIPTION ="" NAME ="Union_Transformation" OBJECTVERSION ="1" REUSABLE ="NO" TEMPLATEID ="303001" TEMPLATENAME ="Union Transformation" TYPE ="Custom Transformation" VERSIONNUMBER ="3">
            <GROUP DESCRIPTION ="" NAME ="OUTPUT" ORDER ="1" TYPE ="OUTPUT"/>
            <GROUP DESCRIPTION ="" NAME ="NEWGROUP" ORDER ="2" TYPE ="INPUT"/>
            <GROUP DESCRIPTION ="" NAME ="NEWGROUP1" ORDER ="3" TYPE ="INPUT"/>
            <GROUP DESCRIPTION ="" NAME ="NEWGROUP2" ORDER ="4" TYPE ="INPUT"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="OUTPUT" NAME ="FIRST_LINE" OUTPUTGROUP ="OUTPUT" PICTURETEXT ="" PORTTYPE ="OUTPUT" PRECISION ="500" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="OUTPUT" NAME ="PARA_out" OUTPUTGROUP ="OUTPUT" PICTURETEXT ="" PORTTYPE ="OUTPUT" PRECISION ="500" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="NEWGROUP" NAME ="FIRST_LINE2" OUTPUTGROUP ="NEWGROUP" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="500" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="NEWGROUP" NAME ="PARA_out2" OUTPUTGROUP ="NEWGROUP" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="500" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="NEWGROUP1" NAME ="FIRST_LINE3" OUTPUTGROUP ="NEWGROUP1" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="500" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="NEWGROUP1" NAME ="PARA_out3" OUTPUTGROUP ="NEWGROUP1" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="500" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="NEWGROUP2" NAME ="FIRST_LINE4" OUTPUTGROUP ="NEWGROUP2" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="500" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" DEFAULTVALUE ="" DESCRIPTION ="" GROUP ="NEWGROUP2" NAME ="PARA_out4" OUTPUTGROUP ="NEWGROUP2" PICTURETEXT ="" PORTTYPE ="INPUT" PRECISION ="500" SCALE ="0"/>
            <TABLEATTRIBUTE NAME ="Language" VALUE ="C"/>
            <TABLEATTRIBUTE NAME ="Module Identifier" VALUE ="pmuniontrans"/>
            <TABLEATTRIBUTE NAME ="Class Name" VALUE =""/>
            <TABLEATTRIBUTE NAME ="Function Identifier" VALUE ="pmunionfunc"/>
            <TABLEATTRIBUTE NAME ="Runtime Location" VALUE =""/>
            <TABLEATTRIBUTE NAME ="Tracing Level" VALUE ="Normal"/>
            <TABLEATTRIBUTE NAME ="Is Partitionable" VALUE ="Across Grid"/>
            <TABLEATTRIBUTE NAME ="Inputs Must Block" VALUE ="NO"/>
            <TABLEATTRIBUTE NAME ="Is Active" VALUE ="YES"/>
            <TABLEATTRIBUTE NAME ="Update Strategy Transformation" VALUE ="NO"/>
            <TABLEATTRIBUTE NAME ="Transformation Scope" VALUE ="Row"/>
            <TABLEATTRIBUTE NAME ="Generate Transaction" VALUE ="NO"/>
            <TABLEATTRIBUTE NAME ="Output Is Repeatable" VALUE ="Never"/>
            <TABLEATTRIBUTE NAME ="Requires Single Thread Per Partition" VALUE ="NO"/>
            <TABLEATTRIBUTE NAME ="Output Is Deterministic" VALUE ="YES"/>
            <INITPROP DESCRIPTION ="" NAME ="Programmatic Identifier for Class Factory" USERDEFINED ="NO" VALUE =""/>
            <INITPROP DESCRIPTION ="" NAME ="Constructor" USERDEFINED ="NO" VALUE =""/>
        </TRANSFORMATION>
 	*/
	// 重写获取output的方法
	protected void getDepOutput(String fieldName, Element fieldlement,
			Set<String> depFieldList)  {

		// 查询输出的分组名
		try {

			List<Element> fieldDependencys = (List<Element>) XPath.selectNodes(
					fieldlement.getParentElement(), "./FIELDDEPENDENCY[@INPUTFIELD ='" + fieldName
							+ "']");

			// 如果有指定依赖的字段
			if (fieldDependencys != null && fieldDependencys.size() > 0) {

				for (int i = 0; i < fieldDependencys.size(); i++) {
					depFieldList.add(fieldDependencys.get(i).getAttributeValue(
							"OUTPUTFIELD"));
				}

			} else {

				// 找出字段的序号
				Element fieldElement = PcUtils.getChildByName(fieldlement.getParentElement(),
						"TRANSFORMFIELD", fieldName, null);
				// 该字段的input分组名
				String group = fieldElement.getAttributeValue("GROUP");

				// 序号
				int order = 0;
				for (int i = 0; i < fieldlement.getParentElement().getChildren("TRANSFORMFIELD")
						.size(); i++) {
					Element child = (Element) fieldlement.getParentElement().getChildren(
							"TRANSFORMFIELD").get(i);
					// 如果是同一组，序号增加
					if (group.equals(child.getAttributeValue("GROUP"))) {
						order++;
						if (fieldName.equals(child.getAttributeValue("NAME"))) {
							break;
						}
					}
				}

				List<Element> groupElements = (List<Element>) XPath
						.selectNodes(fieldlement.getParentElement(), "./GROUP[@TYPE ='OUTPUT']");
				for (int i = 0; i < groupElements.size(); i++) {
					String outputGroup = groupElements.get(i)
							.getAttributeValue("NAME");

					List<Element> outputElements = (List<Element>) XPath
							.selectNodes(fieldlement.getParentElement(), "./TRANSFORMFIELD[@GROUP ='"
									+ outputGroup + "']");
					depFieldList.add(outputElements.get(order - 1)
							.getAttributeValue("NAME"));
				}
			}
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
