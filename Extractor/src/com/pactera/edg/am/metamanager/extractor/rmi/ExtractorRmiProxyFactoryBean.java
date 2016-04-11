/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.rmi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.remoting.rmi.RmiProxyFactoryBean;

/**
 * 采集端需要访问到Web端的RMI服务器进行数据交流，因此需要知道Web端的RMI服务器的IP与Port。<br>
 * 由于Web端RMI服务器的IP和Port由Web服务器在发起采集之前推送过来，
 * 并且该IP和Port的值还可能被用户在系统配置中修改，因此需要扩展默认的RmiProxyFactoryBean，
 * 以便能随时改变连接Web端RMI的Stub实例，保证通讯正常，并不用重启采集端应用程序。
 * @author fbchen
 * @version 1.0 2010-07-22
 */
public class ExtractorRmiProxyFactoryBean extends RmiProxyFactoryBean {
	private static Log log = LogFactory.getLog(ExtractorRmiProxyFactoryBean.class);
	
	private String servicePath;
	
	public String getServicePath() {
		return servicePath;
	}

	public void setServicePath(String servicePath) {
		this.servicePath = servicePath;
	}
 
	/**
	 * 创建采集端访问RMI的代理。默认IP和Port是localhost:8080。
	 */
	public ExtractorRmiProxyFactoryBean() {
		super();
		this.setServerHostAndPort("localhost", 8888);
	}
	
	/**
	 * 设置MM的rmi服务器的ip和port，若有变化则重新创建Stub。<br>
	 * 该方法可能被采集端的接收程序在接收到服务端的IP和Port后调用，以便实时更新连接。
	 * @param host IP地址
	 * @param port 端口
	 */
	public void setServerHostAndPort(String host, int port) {
		String url = "rmi://"+host+":"+port+"/" + (servicePath!=null ? servicePath : "");
		if (!url.equals(this.getServiceUrl())) {
			this.setServiceUrl(url);
			this.refreshStub();
		}
	}
	
	
	/**
	 * 刷新Stub，重建连接
	 */
	protected void refreshStub() {
		try {
			// 父类没有提供重新创建Stub的方法，而且很多变量是private的，没办法o(╯□╰)o
			this.refreshAndRetry(null);
		} catch (NullPointerException e) {
			//MethodInvocation is null
		} catch (Throwable e) {
			log.error("重建RMI连接失败", e);
		}
	}
	
}
