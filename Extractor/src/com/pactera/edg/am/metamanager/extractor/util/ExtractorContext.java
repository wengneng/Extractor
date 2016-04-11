/*
 * Copyright 2009 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, BeiJing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

package com.pactera.edg.am.metamanager.extractor.util;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;

import net.sf.saxon.FeatureKeys;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.xml.sax.InputSource;

/**
 * 适配器上下文
 * 
 * @author fbchen
 * @version 1.0 Date: 2009-8-14 下午08:39:30
 * @category
 * 修改人：王磊
 * 修改时间：2009-09-10
 * 修改内容：将getXsltTemplate方法、getTransformer方法抛出异常由Exception改为TransformerConfigurationException
 * 
 */
public class ExtractorContext {

	static Log log = LogFactory.getLog(ExtractorContext.class);

	private static ExtractorContext instance = new ExtractorContext();

	/**
	 * 存放XSLT文件的类路径、模版在内存中
	 */
	// private final Map<String, Templates> templates = new HashMap<String,
	// Templates>();
	/**
	 * 私有构造器，单例
	 */
	private ExtractorContext()
	{
		super();
	}

	/**
	 * getInstance
	 * 
	 * @return this
	 */
	public static ExtractorContext getInstance() {
		return instance;
	}

	/**
	 * 获取内存中的模版。需要提供XSLT的类路径。
	 * 
	 * @return the xsltTemplate throws Exception 初始化XLST模版失败
	 */
	public Templates getXsltTemplate(String xslt) throws IOException, TransformerConfigurationException {
		// if (!this.templates.containsKey(xslt)) {
		InputStream in = new DefaultResourceLoader().getResource(xslt).getInputStream();
		Source xsltSource = new SAXSource(new InputSource(in));
		TransformerFactory factory = TransformerFactory.newInstance();
		factory.setAttribute(FeatureKeys.DTD_VALIDATION, Boolean.FALSE); //TODO 去除校验DTD，为啥不起作用？
		factory.setAttribute(FeatureKeys.SCHEMA_VALIDATION, 4);
		Templates xsltTemplate = factory.newTemplates(xsltSource);
		// this.templates.put(xslt, xsltTemplate);
		if (in != null)
			in.close();
		// }
		return xsltTemplate;
	}

	/**
	 * 获取内存中的模版对应的转换器。需要提供XSLT的类路径。
	 * 
	 * @return new Transformer
	 * @throws Exception
	 *             初始化XLST模版失败
	 */
	public Transformer getTransformer(String xslt) throws IOException, TransformerConfigurationException {
		return getXsltTemplate(xslt).newTransformer();
	}

}
