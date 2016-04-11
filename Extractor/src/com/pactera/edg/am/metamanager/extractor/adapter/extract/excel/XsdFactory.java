package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import com.pactera.edg.am.metamanager.app.bo.Attribute;
import com.pactera.edg.am.metamanager.app.bo.Classifier;
import com.pactera.edg.am.metamanager.app.bo.Datatype;
import com.pactera.edg.am.metamanager.app.bo.EnumItem;
import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;

/**
 * XSD验证文件工厂类
 * 
 * @author wanglei 2009-09-22
 * 
 */
public class XsdFactory {
	IClassifierQueryBS qbs = null;
	XMLStreamWriter temp_xsw = null;
	// classList用以记录所有模板涉及的类，出现多次的仅记录一次。
	ArrayList<String> classList = new ArrayList<String>();
	private int deepth = 0;
	private boolean isExcelTemplet = true;

	// /**
	// * @param args
	// */
	// public static void main(String[] args) {
	// //TestClassQ tc = new TestClassQ();
	// try{
	//			
	// XsdFactory xf = XsdFactory.newInstance();
	// //XMLOutputFactory f = XMLOutputFactory.newInstance();
	// /*
	// xf.temp_xsw.writeStartDocument();
	// xf.setFeatures(tc.getTestCls());
	// xf.temp_xsw.writeEndDocument();
	// xf.temp_xsw.close();
	// */
	// HashMap<String,ArrayList<String>> map = new
	// HashMap<String,ArrayList<String>>();
	// ArrayList<String> sunList = new ArrayList<String>();
	// sunList.add("Column");
	// map.put("Table", sunList);
	// map.put("View", sunList);
	// ArrayList<String> rootList = new ArrayList<String>();
	// rootList.add("Table");
	// rootList.add("View");
	// xf.qbs = new TestClassifierQueryBS();
	//			
	// String filename = "D:\\test.txt";
	// xf.gernateXsdFile(filename, map, rootList);
	// }catch(Exception e){
	// e.printStackTrace();
	// }
	// }

	private XsdFactory() {
	}

	public static XsdFactory newInstance() throws Exception {
		XsdFactory xf = new XsdFactory();
		
		return xf;
	}

	/**
	 * 构造XSD文件，根节点为root，有四个固定的子节点，分别是instances、relationships、templetid、hookpath，后两个节点为Excel模板专有
	 * 
	 * @param map
	 *            父子关系，key为父,value为子
	 * @return
	 * @throws Exception
	 */
	public void gernateXsdFile(String filename,
			HashMap<String, ArrayList<String>> map, ArrayList<String> firstLevel)
			throws Exception {
		XMLOutputFactory f = XMLOutputFactory.newInstance();
		File file = new File(filename);
		FileOutputStream fos = new FileOutputStream(file);
		temp_xsw = f.createXMLStreamWriter(fos);
		this.temp_xsw.writeStartDocument();
		temp_xsw.writeStartElement("xs:schema");
		// 设置命名空间
		temp_xsw.writeAttribute("xmlns:xs", "http://www.w3.org/2001/XMLSchema");
		temp_xsw.writeAttribute("elementFormDefault", "qualified");
		temp_xsw.writeAttribute("attributeFormDefault", "unqualified");
		// 设置root节点
		temp_xsw.writeStartElement("xs:element");
		temp_xsw.writeAttribute("name", "root");
		temp_xsw.writeStartElement("xs:complexType");
		temp_xsw.writeStartElement("xs:sequence");
		// 设置root子节点instances
		this.setInstancesTag(map, firstLevel);

		// 设置root子节点relationships
		setRelationshipsTag();
		if (isExcelTemplet) {
			// 针对Excel，设置root子节点templetid、hookpath
			setTempletIdTag();
			setHookPathTag();
		}
		// 设置root节点的结束标识
		temp_xsw.writeEndElement();// xs:element root
		temp_xsw.writeEndElement();// xs:complexType root
		temp_xsw.writeEndElement();// sequence root

		// 对instances中多次出现的子节点，为减少冗余，采用了引用的形式，本处定义被引用的节点

		// 设置instances中引用到的元数据属性集合
		// classList记录了所有出现的类，此处针对不同的类生成type，用以校验属性及属性值是否合法
		Iterator<String> itt = classList.iterator();
		while (itt.hasNext()) {
			setFeaturesTag(itt.next());
		}
		// 设置instances中引用到的Attribute节点
		setAttributeTag();

		temp_xsw.writeEndElement();// xs:schema
		this.temp_xsw.writeEndDocument();
		this.temp_xsw.close();

		return;
	}

	private void setTempletIdTag() throws Exception {
		temp_xsw.writeStartElement("xs:element");
		temp_xsw.writeAttribute("name", "templetid");//
		// temp_xsw.writeAttribute("minOccurs", "0");// <xs:element
		// name="templetid" minOccurs="0">
		temp_xsw.writeStartElement("xs:annotation");// <xs:annotation>
		temp_xsw.writeStartElement("xs:documentation");// <xs:documentation>
		temp_xsw.writeCharacters("Excel模板编号，仅当转换Excel时存在");//
		temp_xsw.writeEndElement();// </xs:documentation>
		temp_xsw.writeEndElement();// </xs:annotation>
		temp_xsw.writeEndElement();// </xs:element>
	}

	private void setHookPathTag() throws Exception {
		temp_xsw.writeStartElement("xs:element");
		temp_xsw.writeAttribute("name", "hookpath");
		// temp_xsw.writeAttribute("minOccurs", "0");// <xs:element
		// name="hookpath" minOccurs="0">
		temp_xsw.writeStartElement("xs:annotation");// <xs:annotation>
		temp_xsw.writeStartElement("xs:documentation");// <xs:documentation>
		temp_xsw.writeCharacters("Excel悬挂点路径，仅当转换Excel时存在");//
		temp_xsw.writeEndElement();// </xs:documentation>
		temp_xsw.writeEndElement();// </xs:annotation>
		temp_xsw.writeEndElement();// </xs:element>
	}

	private void setAttributeTag() throws Exception {
		temp_xsw.writeStartElement("xs:element");
		temp_xsw.writeAttribute("name", "attributes");// <xs:element
		// name="attributes">
		temp_xsw.writeStartElement("xs:complexType");// <xs:complexType>
		temp_xsw.writeStartElement("xs:sequence");// <xs:sequence>
		temp_xsw.writeStartElement("xs:element");
		temp_xsw.writeAttribute("name", "instanceid");// <xs:element
		// name="instanceid">
		temp_xsw.writeStartElement("xs:simpleType");// <xs:simpleType>
		temp_xsw.writeEmptyElement("xs:restriction");
		temp_xsw.writeAttribute("base", "xs:string");// <xs:restriction
		// base="xs:string"/>
		temp_xsw.writeEndElement();// </xs:simpleType>
		temp_xsw.writeEndElement();// </xs:element>
		temp_xsw.writeStartElement("xs:element");//							
		temp_xsw.writeAttribute("name", "instancecode");// <xs:element
		// name="instancecode">
		temp_xsw.writeStartElement("xs:simpleType");// <xs:simpleType>
		temp_xsw.writeStartElement("xs:restriction");
		temp_xsw.writeAttribute("base", "xs:string");// <xs:restriction
		// base="xs:string"/>
		temp_xsw.writeEmptyElement("xs:maxLength");
		temp_xsw.writeAttribute("value", "200");// <xs:maxLength value="200"/>
		temp_xsw.writeEndElement();// </xs:restriction>
		temp_xsw.writeEndElement();// </xs:simpleType>
		temp_xsw.writeEndElement();// </xs:element>
		temp_xsw.writeStartElement("xs:element");//							
		temp_xsw.writeAttribute("name", "instancename");// <xs:element
		// name="instancename">
		temp_xsw.writeStartElement("xs:simpleType");// <xs:simpleType>
		temp_xsw.writeStartElement("xs:restriction");
		temp_xsw.writeAttribute("base", "xs:string");// <xs:restriction
		// base="xs:string"/>
		temp_xsw.writeEmptyElement("xs:maxLength");
		temp_xsw.writeAttribute("value", "500");// <xs:maxLength value="200"/>
		temp_xsw.writeEndElement();// </xs:restriction>
		temp_xsw.writeEndElement();// </xs:simpleType>
		temp_xsw.writeEndElement();// </xs:element>
		temp_xsw.writeEndElement();// </xs:sequence>
		temp_xsw.writeEndElement();// </xs:complexType>
		temp_xsw.writeEndElement();// </xs:element>
	}

	private void setRelationshipsTag() throws Exception {
		temp_xsw.writeStartElement("xs:element");// <xs:element
		// name="relationships">
		temp_xsw.writeAttribute("name", "relationships");//
		temp_xsw.writeStartElement("xs:complexType");// <xs:complexType>
		temp_xsw.writeStartElement("xs:sequence");// <sequence>
		temp_xsw.writeStartElement("xs:element");// <xs:element
		// name="relationship"
		// minOccurs="0"
		// maxOccurs="unbounded">
		temp_xsw.writeAttribute("name", "relationship");//
		temp_xsw.writeAttribute("minOccurs", "0");//
		temp_xsw.writeAttribute("maxOccurs", "unbounded");//
		temp_xsw.writeStartElement("xs:complexType");// <xs:complexType>
		temp_xsw.writeEmptyElement("xs:attribute");
		temp_xsw.writeAttribute("name", "frominstanceid");//
		temp_xsw.writeAttribute("use", "required");// <xs:attribute
		// name="frominstanceid"
		// use="required"/>
		temp_xsw.writeEmptyElement("xs:attribute");
		temp_xsw.writeAttribute("name", "toinstanceid");//
		temp_xsw.writeAttribute("use", "required");// <xs:attribute
		// name="toinstanceid"
		// use="required"/>
		temp_xsw.writeEmptyElement("xs:attribute");
		temp_xsw.writeAttribute("name", "deprole");// <xs:attribute
		// name="deprole"/>
		temp_xsw.writeEmptyElement("xs:attribute");
		temp_xsw.writeAttribute("name", "depedrole");// <xs:attribute
		// name="depedrole"/>
		temp_xsw.writeEndElement();// </xs:complexType>
		temp_xsw.writeEndElement();// </xs:element>
		temp_xsw.writeEndElement();// </xs:sequence>
		temp_xsw.writeEndElement();// </xs:complexType>
		temp_xsw.writeEndElement();// </xs:element>
	}

	private void setInstancesTag(HashMap<String, ArrayList<String>> map,
			ArrayList<String> firstLevel) throws Exception {
		Iterator<String> it = firstLevel.iterator();
		String classId = null;

		temp_xsw.writeStartElement("xs:element");
		temp_xsw.writeAttribute("name", "instances");
		temp_xsw.writeStartElement("xs:complexType");
		temp_xsw.writeStartElement("xs:sequence");

		// <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
		// elementFormDefault="qualified" attributeFormDefault="unqualified">
		while (it.hasNext()) {
			classId = it.next();
			setInstanceTag(classId, map);
		}
		temp_xsw.writeEndElement();// /sequence
		temp_xsw.writeEndElement();// /complexType
		temp_xsw.writeEndElement();// /element
	}

	/**
	 * 递归函数
	 * 
	 * @param fatherClassID
	 * @param map
	 * @throws Exception
	 */
	private void setInstanceTag(String fatherClassID,
			HashMap<String, ArrayList<String>> map) throws Exception {
		String classId = fatherClassID;
		ArrayList<String> childrenList = null;
		this.deepth++;
		// Classifier cls = qbs.getClassifier(classId);

		// 首先拼接其自身数据部分
		this.setInstanceTagStart(classId);

		// 构造Class下的属性校验格式，class可能出现多次。
		if (!classList.contains(classId)) {
			classList.add(classId);
		}

		// 而后查询其子节点，并递归调用
		childrenList = map.get(classId);

		if (childrenList != null) {
			Iterator<String> it = childrenList.iterator();
			if (it != null) {
				temp_xsw.writeStartElement("xs:element");
				temp_xsw.writeAttribute("name", "instances");
				temp_xsw.writeAttribute("minOccurs", "0");
				temp_xsw.writeStartElement("xs:complexType");
				temp_xsw.writeStartElement("xs:sequence");
				while (it.hasNext()) {
					classId = it.next();
					setInstanceTag(classId, map);
				}
				temp_xsw.writeEndElement(); // xs:sequence
				temp_xsw.writeEndElement(); // xs:complexType
				temp_xsw.writeEndElement(); // xs:element
			}
		}

		this.setInstanceTagEnd(classId);
		this.deepth--;
	}

	/**
	 * 创建不同类的XSD约束
	 * 
	 * @param cls
	 * @throws Exception
	 */
	private void setInstanceTagStart(String clsId) throws Exception {
		String pathPattern = "*/*";// 路径匹配

		temp_xsw.writeStartElement("xs:element"); // xs:element
		temp_xsw.writeAttribute("name", clsId);
		temp_xsw.writeAttribute("maxOccurs", "unbounded");
		temp_xsw.writeAttribute("minOccurs", "0");
		temp_xsw.writeStartElement("xs:complexType"); // xs:complexType

		temp_xsw.writeStartElement("xs:sequence");
		temp_xsw.writeEmptyElement("xs:element");
		temp_xsw.writeAttribute("ref", "attributes");
		temp_xsw.writeEmptyElement("xs:element");
		temp_xsw.writeAttribute("name", "features");
		temp_xsw.writeAttribute("type", clsId);

		// path
		if (this.deepth == 1) {
			temp_xsw.writeStartElement("xs:element");
			temp_xsw.writeAttribute("name", "path");
			temp_xsw.writeStartElement("xs:simpleType");
			temp_xsw.writeStartElement("xs:restriction");
			temp_xsw.writeAttribute("base", "xs:string");
			temp_xsw.writeCharacters("\n");
			temp_xsw.writeEmptyElement("xs:pattern");
			temp_xsw.writeAttribute("value", pathPattern);
			temp_xsw.writeEndElement();
			temp_xsw.writeEndElement();
			temp_xsw.writeEndElement();
		}
	}

	/**
	 * 
	 * @throws Exception
	 */
	private void setInstanceTagEnd(String classId) throws Exception {
		temp_xsw.writeEndElement(); // </xs:sequence>
		// Instance Tag 's Attribute
		temp_xsw.writeEmptyElement("xs:attribute");
		temp_xsw.writeAttribute("name", "composition");
		temp_xsw.writeAttribute("type", "xs:string");
		temp_xsw.writeStartElement("xs:attribute");
		temp_xsw.writeAttribute("name", "class");
		temp_xsw.writeAttribute("use", "required");
		temp_xsw.writeStartElement("xs:simpleType");
		temp_xsw.writeStartElement("xs:restriction");
		temp_xsw.writeAttribute("base", "xs:string");
		temp_xsw.writeEmptyElement("xs:enumeration");
		temp_xsw.writeAttribute("value", classId);
		temp_xsw.writeEndElement();
		temp_xsw.writeEndElement();
		temp_xsw.writeEndElement();

		temp_xsw.writeEndElement(); // </xs:complexType>
		temp_xsw.writeEndElement(); // </xs:element>
	}

	/**
	 * 设置类属性
	 * 
	 * @param cls
	 * @throws Exception
	 */
	private void setFeaturesTag(String clsId) throws Exception {
		Classifier cls = qbs.getClassifier(clsId);
		String fName = clsId;
		Attribute att = null;
		temp_xsw.writeStartElement("xs:complexType");
		temp_xsw.writeAttribute("name", fName);
		temp_xsw.writeStartElement("xs:sequence");
		Iterator<Attribute> it = cls.getAttributes().iterator();
		while (it.hasNext()) {
			att = it.next();
			temp_xsw.writeStartElement("xs:element");
			temp_xsw.writeAttribute("name", att.getId());
			temp_xsw.writeStartElement("xs:simpleType");
			temp_xsw.writeAttribute("id", fName + att.getId());
			this.setRestriction(att);
			temp_xsw.writeEndElement();
			temp_xsw.writeEndElement();
		}
		temp_xsw.writeEndElement();
		temp_xsw.writeEndElement();
	}

	/**
	 * 设置某一属性的具体约束，包括基本类型和附加的约束
	 * 
	 * @param att
	 * @throws Exception
	 */
	private void setRestriction(Attribute att) throws Exception {
		Datatype dt = att.getDatatype();
		String id = dt.getId();
		temp_xsw.writeStartElement("xs:restriction");

		// string类型，需要关注length属性
		if (id.equals("string")) {
			temp_xsw.writeAttribute("base", "xs:string");
			temp_xsw.writeEmptyElement("xs:maxLength");
			temp_xsw.writeAttribute("value", att.getLength().toString());
		}

		// int类型，需要关注length属性
		if (id.equals("integer")) {
			temp_xsw.writeAttribute("base", "xs:int");
			temp_xsw.writeEmptyElement("xs:minInclusive");
			temp_xsw.writeAttribute("value", att.getMin());
			temp_xsw.writeEmptyElement("xs:maxInclusive");
			temp_xsw.writeAttribute("value", att.getMax());
		}

		// float类型，需要关注Precision属性,如 10.2
		if (id.equals("float")) {
			temp_xsw.writeAttribute("base", "xs:decimal");
			String num[] = att.getPrecisionDigit().split(".");
			String total = num[1];
			String xiaoshu = num[2];

			temp_xsw.writeEmptyElement("xs:fractionDigits");
			temp_xsw.writeAttribute("value", xiaoshu);
			temp_xsw.writeEmptyElement("xs:totalDigits");
			temp_xsw.writeAttribute("value", total);
		}

		// time类型
		if (id.equals("time")) {
			temp_xsw.writeAttribute("base", "xs:string");
			temp_xsw.writeEmptyElement("xs:pattern");
			// 匹配时间格式
			temp_xsw.writeAttribute("value", "yyyy-mm-dd");
		}

		// 枚举类型
		if (id.equals("Enumeration")) {
			temp_xsw.writeAttribute("base", "xs:string");
			// 枚举项的集合
			Iterator<EnumItem> it = att.getDatatype().getEnum_().getEnumItems()
					.iterator();
			while (it.hasNext()) {
				temp_xsw.writeEmptyElement("xs:enumeration");
				temp_xsw.writeAttribute("value", it.next().getId().getItemId());
			}
			// <xs:enumeration value="/ETLFolder"/>
		}

		temp_xsw.writeEndElement();
	}

}
