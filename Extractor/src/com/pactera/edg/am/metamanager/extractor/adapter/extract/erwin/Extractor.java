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
package com.pactera.edg.am.metamanager.extractor.adapter.extract.erwin;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import com.pactera.edg.am.metamanager.extractor.util.XSLTUtil;

/**
 * Erwin适配器(该类没有用到)
 *
 * @author fbchen
 * @version 1.0  Date: 2009-8-14 下午08:59:41
 */
public class Extractor {

	private static final String XSLT = "extractor/adapter/xsl/erwin.xsl";
	
	/**
	 * 构造器
	 */
	public Extractor() {
		super();
	}

	/**
	 * 抽取Erwin-XML数据
	 * @param xmlFileInput 输入
	 * @param output 输出
	 * @throws Exception 转换异常
	 */
	public void extract(InputStream xmlFileInput, OutputStream output) throws Exception {
		XSLTUtil.transform(xmlFileInput, output, XSLT);
	}
	
	/**
	 * 抽取Erwin-XML数据
	 * @param xmlFile 输入
	 * @param output  输出
	 * @throws Exception 转换异常
	 */
	public void extract(File xmlFile, OutputStream output) throws Exception {
		XSLTUtil.transform(xmlFile, output, XSLT);
	}
	
}
