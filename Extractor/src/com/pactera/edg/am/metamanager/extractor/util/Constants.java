package com.pactera.edg.am.metamanager.extractor.util;

/**
 * 采集使用的公共常量
 * 1.配置文件采用类获取资源的机制获取,如Class.getResource(conf).getPath()
 * 2.类获取资源的方式,须采用"/"分隔
 * 
 * @author user
 * @version 1.0 Date: Jul 6, 2009
 * 
 */
public class Constants {

	// 元数据路径分隔符
	public final static String METADATA_PATH_SEPARATOR = "/";
	
	// 元数据重复分隔符
	public final static String METADATA_DUPLICATE_SEPARATOR = ".";
	

	// RMI配置文件所在路径
	public final static String RMI_SERVER_CONFIG_PATH = "/extractor/spring/" +
			"context-adapter-rmi-server.xml";
	
	// spring配置文件的目录位置
	public final static String SPRING_DIRECTORY = "/extractor/spring/";

	// 采集模块所需的配置文件的路径
	public final static String EXTRACTOR_CONF_PATH = "/extractor/spring/context-service.xml";
	
	// 采集模块所需的配置文件的路径(rmi客户端配置)
	public final static String EXTRACTOR_RMI_CONF_PATH = "/extractor/spring/context-adapter-rmi-client.xml";
	
	// 存放SQL的配置文件路径
	public final static String SQL_STORE_PATH = "extractor/dbscript/{0}/harvestSql.xml";
	
	// 存放SQL的配置文件路径
	public final static String TRANSFORM_CONF_PATH = "extractor/transform.conf.properties";
	
	// SQL中表示属性的标识
	public final static String SQL_ATTR_KEYS_FLAG = "#ATTRS_KEYS#";
	// SQL中表示属性的标识
	public final static String SQL_ATTR_HARVEST_KEYS_FLAG = "#ATTRS_HARVEST_KEYS#";
	
	// // SQL中表示父结点的标识
	public final static String PARENT_METADATA_ID = "#PARENT_METADATA_ID#";

	// SQL语句中的参数
	public final static String  SQL_START_TIME = "#START_TIME#";
	
	// SQL语句中的参数
	public final static String  SQL_INSTANCE_ID = "#INSTANCE_ID#";
	// SQL语句中的参数
	public final static String  SQL_TASK_INSTANCE_ID = "#TASK_INSTANCE_ID#";
	// SQL语句中的参数
	public final static String  SQL_NAMESPACE = "#NAMESPACE#";

	// SQL语句中的参数
	public final static String  SQL_END_TIME = "#END_TIME#";

	// SQL语句中的参数
	public final static String  SQL_OPER_TYPE = "#OPER_TYPE#";
	
	// SQL中表示属性值的标识
	public final static String SQL_ATTR_VALUES_FLAG = "#ATTRS_VALUES#";
	
	public final static String SQL_FLAG = "/extractor/sql";
	
	// 适配器配置文件的相对路径的前缀
	public final static String ADAPTER_RELATIVE_PATH_PREFIX = "/extractor/spring/context-adapter-";

	// 适配器配置文件的相对路径的后缀
	public final static String ADAPTER_RELATIVE_PATH_SUFFIX = ".xml";
	
	public final static String LOG4J_CONF_PATH = "/extractor/log4j.properties";
	
	// 采集模块通用配置
	public final static String EXTRACTOR_COMMON_CONF = "extractor/extractor-conf.properties";
	
	// sequence一次增加的值,批量操作时可以用此值实现批量插入,从而减少访问sequence的次数
	public final static int INCREMENT_SIZE = 20;
	
	
	// 查询SQL时列表中的最大表达式数
	public final static int MAX_EXPRESSION_SIZE = 500;
	
	/**
	 * 批量写日志的最大缓存数
	 */
	public final static int MAX_EXTRACTORLOG_SIZE = 1000;
	
	/**
	 * 变更信息详细内容的最在记录数,库表中为4000,考虑到中文存储的情况,(1个中文占三个字符),取1300
	 * 考虑到TD只设置了1500个字符,故这里设置为1500/3=500
	 */
	public final static int MAX_ALTER_METADATA_ATTR_SIZE = 500;
	
	/**
	 * 存储元数据名称的最大长度，500/3=166
	 */
	public final static int MAX_METADATA_NAME_SIZE = 166;

	/**
	 * 存储元数据CODE的最大长度，200/3=66
	 */
	public final static int MAX_METADATA_CODE_SIZE = 66;
	
	/**
	 * 记录日志是默认最大长度,4000/3=1333
	 */
	public final static int MAX_LOG_SIZE = 1333;

	/** 
	 * 元数据差异分析最大数
	 */
	public final static int MAX_INCREMENT_CNT = 50000;

	public final static int MAX_INCREMENT_CACHE = 10000;
	
	// 元数据的最大个数,此个数需根据其namespace获取子孙结点,如判断过大,则考虑采用另的方式实现
	public final static int MAX_METADATA_SIZE = 10;

}
