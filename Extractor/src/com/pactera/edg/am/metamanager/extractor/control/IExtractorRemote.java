package com.pactera.edg.am.metamanager.extractor.control;

import java.util.Properties;

import com.pactera.edg.am.metamanager.extractor.adapter.support.TestConnectionException;
import com.pactera.edg.am.metamanager.extractor.bo.ServerDomain;
import com.pactera.edg.am.metamanager.extractor.ex.HarvestAdapterException;

/**
 * 采集元数据接口,作为RMI服务器端与客户端交互的接口
 * 
 * @author chenhq
 * @version 1.0
 */
public interface IExtractorRemote {

	final String SPRING_NAME = "extractorClient";

	/**
	 * 检查RMI是否正在运行
	 * @return TRUE-是
	 * @throws java.rmi.ConnectException RMI没有在运行
	 */
	boolean isRunning();
	
	/**
	 * 报告采集端的RMI的状态，返回XML格式数据
	 * @return XML
	 */
	String reportState();
	
	/**
	 * 采集元数据接口
	 * 
	 * @param taskInstanceId
	 *            采集任务实例ID,由客户端传输过来
	 * @return ServerDomain RMIServer返回给客户端的对象
	 */
	ServerDomain adapterMetadata(String taskInstanceId) throws HarvestAdapterException;
	
	/**
	 * 中断采集的方法
	 * @return 中断成功则返回true,否则返回false
	 */
	boolean stop();
	
	/**
	 * 审核入库接口
	 * @param taskInstanceId　任务实例ID
	 * @return　入库成功则返回TRUE
	 */
	boolean auditLoad(String taskInstanceId);

	/**
	 * 接收Web服务器的RMI的对外IP和端口，若Extractor还没有知道则需要根据此信息创建Stub
	 * @param host Web服务器的IP
	 * @param port Web服务器的端口
	 * @author fbchen 2010-07-22
	 */
	void receiveServerHostAndPort(String host, Integer port);

	/**
	 * 接收Web服务器的数据库连接信息，如驱动、用户、密码、连接串
	 * @param properties 数据库连接信息
	 */
	void receiveJdbcProperties(Properties properties);

	/**
	 * 根据适配器的Spring配置文件和相关的配置参数，进行测试连接。
	 * @param springFiles SPRING配置文件
	 * @param properties 连接配置属性
	 */
	boolean testConnect(String[] springFiles, Properties properties) throws TestConnectionException;
	
}
