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
import java.util.Properties;

/**
 * 转换表名配置文件加载器
 * 
 * @author user
 * @version 1.0 Date: Feb 1, 2010
 * 
 */
public class TransformConfLoader {
	/**
	 * 存储表名转换配置
	 */
	private static Properties properties;

	private TransformConfLoader()
	{
	}

	public static Properties getProperties() throws IOException {
		if (properties == null)
			loadTransformConf();
		return properties;
	}

	/**
	 * 将资源中的表名转换配置存储于指定内存中
	 */
	private static void loadTransformConf() throws IOException {

		properties = new Properties();
		properties.load(TransformConfLoader.class.getClassLoader().getResourceAsStream(Constants.TRANSFORM_CONF_PATH));

	}

	public static void main(String[] args) {
		try {
			TransformConfLoader.getProperties();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
