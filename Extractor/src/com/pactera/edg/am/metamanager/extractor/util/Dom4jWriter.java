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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

/**
 * Dom4j写入器
 * 
 * @author user
 * @version 1.0 Date: Aug 14, 2009
 * 
 */
public class Dom4jWriter {
	/**
	 * 将XML内容写入指定文件中。
	 * 
	 * @param document
	 * @param file
	 */
	public static void writeDocument(Document document, File file) {
		try {
			OutputFormat format = OutputFormat.createPrettyPrint();
			XMLWriter writer = new XMLWriter(new FileOutputStream(file), format);
			writer.write(document);
			writer.close();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Document createDocument() {
		Document document = DocumentHelper.createDocument();
		Map<String, String> map = new TreeMap<String, String>();
		map.put("name", "");
		map.put("signature", "");
		map.put("version", "1.0.1");
		document.addProcessingInstruction("mm", map);
		
		return document;
	}

	public static Element addElement(Element element, String elementName) {
		return element.addElement(elementName);
	}

	public static void addAttribute(Element element, String key, String value) {
		element.addAttribute(key, value);
	}
}
