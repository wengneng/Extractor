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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

/**
 * xpath操作封装类
 * 
 * @author user
 * @version 1.0 Date: Jul 9, 2009
 * 
 */
public class Dom4jReader {
	private final static String UTF_8 = "UTF-8";

	private final static String UTF_16LE = "UTF-16LE";

	private final static String UTF_16BE = "UTF-16BE";

	private final static String UTF8 = "UTF-8";

	private Document document;

	/**
	 * 
	 * @param file
	 * @return
	 * @throws FileNotFoundException
	 */
	public boolean initDocument(File file) throws FileNotFoundException {
		InputStream input = new FileInputStream(file);
		return initDocument(input);
	}

	/**
	 * 
	 * @param file
	 * @return
	 */
	public boolean initDocument(InputStream input) {
		SAXReader xmlReader = new SAXReader();
		try {

			document = xmlReader.read(new BufferedReader(createReader(input, "UTF-8")));
			return true;
		}
		catch (DocumentException e) {
			e.printStackTrace();
		}
		finally {
		}
		return false;
	}

	private Reader createReader(InputStream input, String encoding) {
		Reader reader = null;
		if (UTF8.equals(encoding)) {
			// 去除BOM头
			try {
				final int bufSize = 1024;
				PushbackInputStream pis = new PushbackInputStream(input, bufSize);

				encoding = getBOMEncoding(pis);
				reader = new InputStreamReader(pis, encoding);
			}
			catch (IOException e) {
				return null;

			}
		}
		else {
			try {
				reader = new InputStreamReader(input, encoding);
			}
			catch (UnsupportedEncodingException e) {
				return null;
			}
		}

		return reader;
	}

	/**
	 * 
	 * 根据输入的InputStream返回对应的BOM。
	 * 
	 * @param pis
	 *            (OUT)PushbackInputStream对象。若is带BOM头，分析后自动跳过BOM。
	 * @return BOM字串(见java.nio.charset.Charset)
	 * @exception
	 */
	public static String getBOMEncoding(PushbackInputStream pis) throws IOException {
		String encoding = UTF8;
		int[] bytes = new int[3];
		bytes[0] = pis.read();
		bytes[1] = pis.read();
		bytes[2] = pis.read();

		if (bytes[0] == 0xFE && bytes[1] == 0xFF) {
			encoding = UTF_16BE;
			pis.unread(bytes[2]);
		}
		else if (bytes[0] == 0xFF && bytes[1] == 0xFE) {
			encoding = UTF_16LE;
			pis.unread(bytes[2]);
		}
		else if (bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF) {
			encoding = UTF_8;
		}
		else {
			for (int i = bytes.length - 1; i >= 0; i--) {
				pis.unread(bytes[i]);
			}
		}

		return encoding;
	}

	public Document getDocument() {
		return document;
	}

	public List selectNodes(String expr) {
		return document.selectNodes(expr);
	}

	public Node selectSignleNode(String expr) {
		return document.selectSingleNode(expr);
	}

	public String getElementText(Element element) {
		return element.getTextTrim();
	}

	public void close() {
//		document.clearContent();
		document = null;
	}
}
