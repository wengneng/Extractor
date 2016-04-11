package com.pactera.edg.am.metamanager.extractor.util;

import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import com.pactera.edg.am.metamanager.extractor.ex.ConfFileNotFoundException;

/**
 * 设置LOG4J的配置信息
 * 
 * @author user
 * @version 1.0 Date: Jul 28, 2009
 */
public class Log4jInit {

	private static boolean hasInit = false;

	private Log4jInit() {

	}

	/**
	 * 初始化日志
	 */
	public static void init() {
		if (!hasInit) {
			try {
				Resource r = new DefaultResourceLoader().getResource(Constants.LOG4J_CONF_PATH);
				Properties properties = PropertiesLoaderUtils.loadProperties(r);
				PropertyConfigurator.configure(properties);
				hasInit = true;
			} catch (Exception e) {
				System.out.println(new ConfFileNotFoundException(Constants.LOG4J_CONF_PATH));
			}
		}
	}
	
	/**
	 * 关闭日志
	 */
	public static void shutdown() {
		LogManager.shutdown();
		System.out.println("Shutdown Log4J success.");
	}

}
