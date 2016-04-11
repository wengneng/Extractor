/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, BeiJing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */

/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.TransformerException;


/**
 * XSLTUtil转换工具类
 *
 * @author fbchen
 * @version 1.0  Date: 2009-9-9 下午05:06:16
 * 修改人：王磊
 * 修改时间：2009-09-10
 * 修改内容：将transform方法抛出异常由Exception改为TransformerException
 */
public class XSLTUtil {

	private final static String FACTORY_KEY = "javax.xml.transform.TransformerFactory";
	private final static String FACTORY_SAXON = "net.sf.saxon.TransformerFactoryImpl";
	
	// privare
	private XSLTUtil() {
		
	}
	
	/**
	 * 适用XSLT转换XML数据
	 * @param xmlFileInput 输入
	 * @param output 输出
	 * @param xslt XSLT的类路径，如com/pactera.edg.am/xslt/abc.xsl
	 * @throws Exception 转换异常
	 */
	public static void transform(InputStream xmlFileInput, OutputStream output, String xslt) throws IOException, TransformerConfigurationException,TransformerException {
		System.setProperty(FACTORY_KEY, FACTORY_SAXON);
		Source xmlSource = new StreamSource(xmlFileInput);
		Transformer transformer = ExtractorContext.getInstance().getTransformer(xslt);
		transformer.transform(xmlSource, new StreamResult(output));
	}
	
	/**
	 * 适用XSLT转换XML数据
	 * @param xmlFile 输入
	 * @param output 输出
	 * @param xslt XSLT的类路径，如com/pactera.edg.am/xslt/abc.xsl
	 * @throws Exception 转换异常
	 */
	public static void transform(File xmlFile, OutputStream output, String xslt) throws IOException, TransformerConfigurationException,TransformerException {
		System.setProperty(FACTORY_KEY, FACTORY_SAXON);
		Source xmlSource = new StreamSource(xmlFile);
		Transformer transformer = ExtractorContext.getInstance().getTransformer(xslt);
		transformer.transform(xmlSource, new StreamResult(output));
	}
	
}
