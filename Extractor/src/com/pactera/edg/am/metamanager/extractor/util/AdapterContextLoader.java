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

import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;

/**
 * 适配器所需的spring资源文件的加载类,spring中用到的属性将通过传值注入:将OS参数备份;将传值参数注入至OS参数中;加载spring
 * context, 同时参数注入采用从OS参数中注入的模式
 * 
 * @author user
 * @version 1.0 Date: Aug 7, 2009
 * 
 */
public class AdapterContextLoader {

	private static Log log = LogFactory.getLog(AdapterContextLoader.class);

	/**
	 * 
	 * 创建Spring的ClassPathXMLApplicationContext容器,注意这里的资源文件中的占位符${var}的值都是通过
	 * prop参数传入进去提供， 而不再由.properties文件提供,这样可以方便动态地设置控制${var}的值。
	 * 
	 * @param configLocations
	 *            application context资源文件
	 * @param props
	 *            提供给application context资源文件中占位符所需要的key/value值
	 * @return ApplicationContext 返回Application
	 *         Context对象,如遇异常,包括资源文件不存在,bean创建失败等,将返回null
	 * @exception
	 */
	public static ApplicationContext createApplicationContext(String[] configLocations, Properties props) {
		// 注:spring会依赖system properties的信息，所以不能覆盖掉旧的.
		Properties bakProps = null;
		String logMsg = null;
		try {
			if (props != null && props.size() > 0) {
				// 备份system properties
				bakProps = System.getProperties();

				for (Object key : props.keySet()) {
					System.setProperty((String) key, props.getProperty((String) key));
				}

				if (log.isDebugEnabled()) {
					StringBuffer sb = new StringBuffer();
					sb.append("提供spring的xml资源文件的${var}的prop值如下:\n");
					for (Object key : props.keySet()) {
						sb.append(key + "=" + System.getProperty((String) key));
						sb.append("\n");
					}
					log.debug(sb.toString());
				}
			}
			return new ClassPathXmlApplicationContext(configLocations);
		}
		catch (BeanDefinitionStoreException bse) {
			// 没有找到文件
			logMsg = new StringBuilder("初始化spring容器失败,没有找到配置文件").append(Arrays.toString(configLocations)).append(
					bse.getMessage()).toString();
			log.error(logMsg, bse);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, logMsg);
			throw bse;
		}
		catch (BeanCreationException ce) {
			// BEAN创建失败,即RMI服务没有启动
			logMsg = new StringBuilder("初始化spring容器失败,采集模块BEAN创建失败,").append(Arrays.toString(configLocations)).append(
					ce.getMessage()).toString();
			log.error(logMsg, ce);
			if (logMsg.indexOf("ORA-01017") > -1 || logMsg.indexOf("Invalid password") > -1) {
				// ORACLE用户名密码不正确的异常
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
						"用户名/口令无效,登陆被拒绝,请确保提供给适配器的用户名密码是正确无误的!");
			}
			else {
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, logMsg);
			}
			throw ce;
		}
		finally {
			// 还原system properties,不管设置成功与否,都需要将配置还原
			if (bakProps != null) {
				System.setProperties(bakProps);
			}
		}
	}
}
