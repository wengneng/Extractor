package com.pactera.edg.am.metamanager.extractor.util;

import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.ex.SpringContextLoadException;

/**
 * 映射转换器工厂，根据客户端信息，产生相应的转换器
 * 
 * @author user
 * @version 1.0
 * 
 */
public final class MapperFactory {

	private static Log log = LogFactory.getLog(MapperFactory.class);

	/**
	 * 根据适配器类型,获取所需的转换器(内含适配器)
	 * 
	 * @param adapterTypeKey
	 *            适配器类型
	 * @return 转换器接口
	 */
	public static IMetadataMappingService createMapper(String adapterTypeKey) throws SpringContextLoadException {

		IMetadataMappingService mapper = null;
		String adapterTypeValue = null;
		AbstractApplicationContext aCtx = null;
		try {
			Configuration prop = new PropertiesConfiguration(Constants.EXTRACTOR_COMMON_CONF);
			adapterTypeValue = (String) prop.getProperty(adapterTypeKey);
			String[] contextFile = getRelativePath(adapterTypeValue);
			aCtx = new ClassPathXmlApplicationContext(contextFile);
			mapper = (IMetadataMappingService) aCtx.getBean(IMetadataMappingService.SPRING_NAME);
			return mapper;

		}
		catch (Exception e) {
			// 采集配置文件加载异常
			if (log.isDebugEnabled()) {
				log.error("", e);
			}
			throw new SpringContextLoadException(new StringBuilder("初始化")
					.append(Constants.ADAPTER_RELATIVE_PATH_PREFIX).append(adapterTypeValue).append(
							Constants.ADAPTER_RELATIVE_PATH_SUFFIX).append("失败").toString(), e);

		}
		finally {
			if (aCtx != null) {
				aCtx.registerShutdownHook();

			}
		}
		// 转换器加载失败

	}

	/**
	 * 根据适配器类型,获取所需的转换器(内含适配器),其spring中的参数从前端设置注入
	 * 
	 * @param adapterTypeKey
	 *            适配器类型
	 * @param properties
	 *            转换器所需的各种注入参数
	 * @return 转换器接口
	 */
	public static IMetadataMappingService createMapper(String[] contextFiles, Properties properties) {

		IMetadataMappingService mapper = null;
		ApplicationContext aCtx = AdapterContextLoader.createApplicationContext(contextFiles, properties);
		if (aCtx == null) { return null; }
		mapper = (IMetadataMappingService) aCtx.getBean(IMetadataMappingService.SPRING_NAME);
		return mapper;

	}

	/**
	 * 获取适配器相对路径:相对路径前缀+适配器类型+相对路径后缀
	 * 
	 * @param adapterType
	 *            适配器类型
	 * @return 适配器相对路径
	 */
	private static String[] getRelativePath(String adapterType) {
		return new String[] { new StringBuilder(Constants.ADAPTER_RELATIVE_PATH_PREFIX).append(adapterType).append(
				Constants.ADAPTER_RELATIVE_PATH_SUFFIX).toString() };
	}
}
