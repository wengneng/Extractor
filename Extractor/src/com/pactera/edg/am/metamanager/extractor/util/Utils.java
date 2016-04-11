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

import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 工具类
 * 
 * @author user
 * @version 1.0 Date: Feb 1, 2010
 * 
 */
public class Utils {
	private static Log log = LogFactory.getLog(Utils.class);

	public static String transformName(String name) {
		try {
			Properties properties = TransformConfLoader.getProperties();
			StringBuffer sb = new StringBuffer();
			for (Enumeration<Object> enumer = properties.keys(); enumer.hasMoreElements();) {
				// 表名转换前的匹配项
				Object expression = enumer.nextElement();

				Pattern regex = Pattern.compile((String) expression);
				Matcher regexMatcher = regex.matcher(name);

				// 表名转换后的匹配项
				String targetExpression = (String) properties.get(expression);

				if (regexMatcher.find()) {
					// 与转换前的匹配项相匹配
					regexMatcher.appendReplacement(sb, targetExpression);
					// 匹配到立马返回
					if (log.isDebugEnabled()) {
						log.debug(new StringBuilder("转换前表名:").append(name).append(" 转换后表名:").append(sb).toString());
					}
					return sb.toString();
				}
			}

		}
		catch (Exception e) {
			log.warn("对表名作转换时发生异常!将保持原表名返回:" + name, e);
			
		}

		return name;
	}
	
	public static String truncate(String srcString, int maxSize){
		return srcString.length() > maxSize ? srcString.substring(0, maxSize) : srcString;
	}

	public static void main(String[] args) {
		System.out.println(Utils.transformName("aa_445556"));
	}
}
