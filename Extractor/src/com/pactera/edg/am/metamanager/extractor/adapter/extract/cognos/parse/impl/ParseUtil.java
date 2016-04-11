package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.jdom.xpath.XPath;

import com.cognos.developer.schemas.bibus._3.BaseClass;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.cognos.developer.schemas.bibus._3.OrderEnum;
import com.cognos.developer.schemas.bibus._3.PropEnum;
import com.cognos.developer.schemas.bibus._3.QueryOptions;
import com.cognos.developer.schemas.bibus._3.Report;
import com.cognos.developer.schemas.bibus._3.SearchPathMultipleObject;
import com.cognos.developer.schemas.bibus._3.Sort;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl.CognosWebServerUtil;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public final class ParseUtil {
	private static Log log = LogFactory.getLog(ParseUtil.class);

	/**
	 * 获取cognos模型指定子元素路径的子元素列表
	 * 
	 * @param document
	 *            the JDOM document built from Listing 2
	 * @param visitedNodeName
	 *            指定要访问的子节点元素名称
	 * @param visitedNodeName
	 *            指定要访问的子节点元素名称
	 * @param namespace
	 *            TODO
	 * @return 返回指定元素路径的子元素列表
	 */
	public static List getChildrenElement(Object document,
			String visitedNodeName, Namespace namespace) {
		List visitElements = null;
		try {
			XPath xpath = XPath.newInstance(visitedNodeName);
			xpath.addNamespace("default", namespace.getURI());
			visitElements = xpath.selectNodes(document);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return visitElements;
	}

	/**
	 * 获取cognos模型指定子元素路径的子元素列表
	 * 
	 * @param document
	 *            the JDOM document built from Listing 2
	 * @param visitedNodeName
	 *            指定要访问的子节点元素名称
	 * @param visitedNodeName
	 *            指定要访问的子节点元素名称
	 * @param namespace
	 *            TODO
	 * @return 返回指定元素路径的子元素列表
	 */
	public static Element getChildElement(Object document,
			String visitedNodeName, Namespace namespace) {
		List<Element> elements = getChildrenElement(document, visitedNodeName,
				namespace);

		if (elements.size() > 0) {
			return (Element) elements.get(0);
		} else {
			return null;
		}

	}

	/**
	 * 从复杂的表达式中提取出标准的表达式，如将[gosales_goretailers].[Orders].[Quantity]*[gosales_goretailers].[Orders].[Unit
	 * sale price]拆分为2表达式
	 * 
	 * @param expressions
	 * @return
	 */
	public static List<String> selectExpressions(String expressions) {
		List<String> expressionList = new ArrayList<String>();

		// 方便统计加了一个空格
		expressions = " " + expressions + " ";

		int indexq = 0;

		for (int i = 0; i < expressions.length(); i++) {
			char cr = expressions.charAt(i);

			// 表达式的开头，记录索引
			if (cr == '[' && expressions.charAt(i - 1) != '.') {
				indexq = i;
			}

			// 表达式的结尾，提取整个表达式
			if (cr == ']' && expressions.charAt(i + 1) != '.') {

				String expression = expressions.substring(indexq, i + 1);

				expressionList.add(expression);

			}

		}

		return expressionList;
	}

	/**
	 * 查询一节点的子节点中含有的标准表达式
	 * 
	 * @param el
	 */
	public static void queryExpressionsInElement(Element el,
			List<String> expressionList) {
		List<Element> childern = el.getChildren();
		for (int i = 0; i < childern.size(); i++) {
			String childernValue = childern.get(i).getValue();

			if (childernValue.indexOf("].[") != -1) {
				expressionList.addAll(ParseUtil
						.selectExpressions(childernValue));
			}

			queryExpressionsInElement(childern.get(i), expressionList);
		}

	}

	/**
	 * 创建表达式
	 * 
	 * @param expressionList
	 * @return
	 */
	public static String buildExpression(String[] expressionList) {

		if (expressionList.length == 2) {
			return "[" + expressionList[0] + "].[" + expressionList[1] + "]";
		}

		if (expressionList.length == 3) {
			return "[" + expressionList[0] + "].[" + expressionList[1] + "].["
					+ expressionList[2] + "]";
		}

		return null;
	}

	public static List<String> uniqueList(List<String> list) {
		List<String> relist = new ArrayList<String>();
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String str = iterator.next();

			if (!relist.contains(str)) {
				relist.add(str);
			}

		}

		return relist;
	}

	public static void printXml(Document doc) {
		Format format = Format.getPrettyFormat();
		format.setEncoding("gb2312");// 设置xml文件的字符为gb2312，解决中文问题
		XMLOutputter xmlout = new XMLOutputter(format);
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		try {
			xmlout.output(doc, bo);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String xmlStr = bo.toString();
		System.out.println(xmlStr);
	}

	public static void printXmlByEl(Element el) {
		System.out.println((el.toString()));
//		List<Element> els=el.getChildren();
//		while(els.size()>0){
//			for (int i = 0; i < els.size(); i++) {
//				Element pel=els.get(i);
//				printXmlByEl(pel);
//			}
//		}
	}
	

	/**
	 * 判断日期一是否晚于日期二
	 * 
	 * @param dateStr1
	 * @param dateStr2
	 */
	public static Boolean isDateAfter(String dateStr1, String dateStr2) {
		String str1 = dateStr1.replace("T", "").replace("Z", "");
		String str2 = dateStr2.replace("T", "").replace("Z", "");
		SimpleDateFormat dateformat = new SimpleDateFormat(
				"yyyy-MM-ddHH:mm:ss.SSS");
		try {
			Date date1 = dateformat.parse(str1);
			Date date2 = dateformat.parse(str2);
			return date1.after(date2);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;

	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}

}