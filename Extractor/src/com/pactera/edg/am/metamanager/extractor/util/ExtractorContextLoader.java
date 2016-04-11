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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;
import com.pactera.edg.am.metamanager.extractor.dao.IExtractorLogDao;
import com.pactera.edg.am.metamanager.extractor.ex.SpringContextLoadException;
import com.pactera.edg.am.metamanager.extractor.log.ExtractorLogObservable;
import com.pactera.edg.am.metamanager.extractor.log.ExtractorLogObserver;
import com.pactera.edg.am.metamanager.extractor.rmi.ExtractorRmiProxyFactoryBean;

/**
 * 采集模块所需的spring资源配置的操作类,单例实现
 * 
 * @author hqchen
 * @version 1.0 Date: Jul 24, 2009
 */
public class ExtractorContextLoader {

	private static Log log = LogFactory.getLog(ExtractorContextLoader.class);

	/**
	 * 采集模块所需的spring资源的context
	 */
	private static AbstractApplicationContext aContext;

	private ExtractorContextLoader() {

	}

	/**
	 * 初始化采集模块的SPRING,采用延迟加载,并单例保存资源
	 * 
	 * @return
	 * @throws SpringContextLoadException
	 */
	public static boolean initContext() throws SpringContextLoadException {
		try {
			if (aContext == null) {
				aContext = new ClassPathXmlApplicationContext(new String[] {
						Constants.EXTRACTOR_CONF_PATH,
						Constants.EXTRACTOR_RMI_CONF_PATH
				});

				// 将关闭容器的动作注册为JVM运行时的一个钩子,则当JVM关闭时,关闭容器的动作将被触发
				aContext.registerShutdownHook();
				// 注册写采集日志句柄
				registerExtractorLogObservable();
			}
			return true;
		}
		catch (BeanDefinitionStoreException bse) {
			// 没有找到文件
			log.error(new StringBuilder("初始化spring容器失败,没有找到配置文件").append(Constants.EXTRACTOR_CONF_PATH).append(",")
					.append(Constants.EXTRACTOR_RMI_CONF_PATH).append(",").append(bse.getMessage()).toString());
			if (log.isDebugEnabled()) {
				log.error(new StringBuilder("初始化spring容器失败,没有找到配置文件").append(Constants.EXTRACTOR_CONF_PATH).append(",")
						.append(Constants.EXTRACTOR_RMI_CONF_PATH).toString(), bse);
			}
			throw new SpringContextLoadException(new StringBuilder("初始化spring容器失败,没有找到配置文件").append(
					Constants.EXTRACTOR_CONF_PATH).append(",").append(Constants.EXTRACTOR_RMI_CONF_PATH).toString(),
					bse);
		}
		catch (BeanCreationException ce) {
			// BEAN创建失败,即RMI服务没有启动
			log.error(new StringBuilder("初始化spring容器失败,采集模块BEAN创建失败,").append(Constants.EXTRACTOR_CONF_PATH).append(
					",").append(Constants.EXTRACTOR_RMI_CONF_PATH).append(",").append(ce.getMessage()).toString());
			if (log.isDebugEnabled()) {
				log.error(new StringBuilder("初始化spring容器失败,采集模块BEAN创建失败").append(Constants.EXTRACTOR_CONF_PATH)
						.append(",").append(Constants.EXTRACTOR_RMI_CONF_PATH).toString(), ce);
			}
			throw new SpringContextLoadException(new StringBuilder("初始化spring容器失败,采集模块BEAN创建失败,").append(
					Constants.EXTRACTOR_CONF_PATH).append(",").append(Constants.EXTRACTOR_RMI_CONF_PATH).toString(), ce);
		}
	}

	/**
	 * 注册采集日志观察者至采集日志主题中
	 */
	private static void registerExtractorLogObservable() {
		// 将采集日志观察者放入观察主题中,供观察主题监控写日志
		try {
			ExtractorLogObservable eObservable = new ExtractorLogObservable();
			new ExtractorLogObserver(eObservable).setExtractorLogDao((IExtractorLogDao) ExtractorContextLoader
					.getBean(IExtractorLogDao.SPRING_NAME));
			AdapterExtractorContext.getInstance().setEObservable(eObservable);
			// 采集日志可用
			AdapterExtractorContext.getInstance().setExtractorLoggerUseable(true);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取SPRING 实体BEAN
	 * 
	 * @param beanName BEAN名
	 * @return 实体BEAN
	 * @throws SpringContextLoadException
	 */
	public static Object getBean(String beanName) throws SpringContextLoadException {
		if (initContext()) { return aContext.getBean(beanName); }
		return null;
	}
	
	/**
	 * 获取访问MM-RMI的客户端代理工厂
	 * @return ExtractorRmiProxyFactoryBean
	 */
	public static ExtractorRmiProxyFactoryBean getRmiProxyFactoryBean() {
		return (ExtractorRmiProxyFactoryBean)getBean("&serverProxy");
	}
	
	/**
	 * 获取数据源。是否只能使用org.apache.commons.dbcp.BasicDataSource了？
	 * @return javax.sql.DataSource
	 */
	public static BasicDataSource getDataSource() {
		return (BasicDataSource)getBean("dataSource");
	}
	
	
	/**
	 * 获取MM端的RMI服务器开放的接口
	 * @return MM开放的服务接口
	 */
	public static IClassifierQueryBS getMMRmiServer() {
		return (IClassifierQueryBS)getBean("serverProxy");
	}
}
