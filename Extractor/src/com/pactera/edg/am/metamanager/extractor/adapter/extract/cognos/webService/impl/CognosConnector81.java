package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl;

import java.net.MalformedURLException;
import java.rmi.RemoteException;
import org.apache.axis.AxisFault;
import org.apache.axis.client.Stub;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cognos.developer.schemas.bibus._3.BiBusHeader;
import com.cognos.developer.schemas.bibus._3.ContentManagerServiceStub;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_ServiceLocator;
import com.cognos.developer.schemas.bibus._3.PropEnum;
import com.cognos.developer.schemas.bibus._3.QueryOptions;
import com.cognos.developer.schemas.bibus._3.ReportService_ServiceLocator;
import com.cognos.developer.schemas.bibus._3.SearchPathMultipleObject;
import com.cognos.developer.schemas.bibus._3.SearchPathSingleObject;
import com.cognos.developer.schemas.bibus._3.Sort;
import com.cognos.developer.schemas.bibus._3.XmlEncodedXML;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl.CognosMappingServiceImpl;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class CognosConnector81 {
	private Log log = LogFactory.getLog(CognosConnector81.class);
	// 创建连接Cognos8服务的对象.
	// Content Manager Service 内容库管理服务Locator对象
	private ContentManagerService_ServiceLocator cmServiceLocator = null;
	// Report Service 报表服务Locator对象
	private ReportService_ServiceLocator reportServiceLocator = null;
	// Content Manager Service 内容库服务对象
	private ContentManagerServiceStub cmService = null;
	private String ip;
	private String port;
	private String userName;
	private String password;
	private String namespace;
	private String anonymous;

	public String getAnonymous() {
		return anonymous;
	}

	public void setAnonymous(String anonymous) {
		this.anonymous = anonymous;
	}

	/**
	 * @param ip
	 * @param port
	 * @return ContentManagerServiceStub
	 * @throws Exception
	 * @roseuid 4A52B4CD0167
	 */
	public ContentManagerServiceStub connectByWebService() {
		// BI Bus
		BiBusHeader bibus = null;
		// 创建Cognos 8服务的service locator
		// content manger service locator 内容管理服务locator
		cmServiceLocator = new ContentManagerService_ServiceLocator();

		log.info("开始连接cognos服务器,ip:" + ip + ",端口：" + port);

		try {
			java.net.URL serverURL = new java.net.URL("http://" + ip + ":"
					+ port + "/p2pd/servlet/dispatch");

			log.info("webService's url:" + serverURL.getPath());

			// 得到Cognos 8的服务，通过stub方法来初始化，得到连接
			// 内容管理服务对象
			cmService = new ContentManagerServiceStub(serverURL,
					cmServiceLocator);

		} catch (MalformedURLException e) {
			return null;
		} catch (AxisFault e) {
			return null;
		}

		if ("false".equalsIgnoreCase(anonymous)) {
			try {
				// 获得连接之后进行登陆
				logOn(userName, password, namespace);
			} catch (RemoteException ex) {
				log.info("cognos连接失败");
				return null;
				// ex.printStackTrace();
			}

			// 得到 biBusHeader SOAP:包含登陆信息的Header

			bibus = (BiBusHeader) ((Stub) cmService).getHeaderObject("",
					"biBusHeader");

			if (bibus != null) {
				log.info("cognos连接成功");
				return cmService;
			} else {
				log.info("帐号验证失败，cognos连接失败");
				return null;
			}

		} else {

			try {
				Sort[] sortBy = { new Sort() };
				PropEnum[] properties = {};
				cmService.query(
						new SearchPathMultipleObject("/content/package"),
						properties, sortBy, new QueryOptions());
				log.info("cognos连接成功");
				return cmService;
			} catch (RemoteException e) {
				log.info("cognos连接失败");
				e.printStackTrace();
				return null;
				// // TODO Auto-generated catch block
				// e.printStackTrace();
			}
		}

	}

	/**
	 * @param userName
	 * @param password
	 * @param namespace
	 * @roseuid 4A52B60002AF
	 */
	private void logOn(String userName, String password, String namespace)
			throws RemoteException {
		// 认证字符串
		StringBuffer credentialXML = new StringBuffer();

		credentialXML.append("<credential>");
		// 名称空间
		credentialXML.append("<namespace>");
		credentialXML.append(namespace);
		credentialXML.append("</namespace>");
		// 用户名
		credentialXML.append("<username>");
		credentialXML.append(userName);
		credentialXML.append("</username>");
		// 密码
		credentialXML.append("<password>");
		credentialXML.append(password);
		credentialXML.append("</password>");

		credentialXML.append("</credential>");
		// 转换为XML编码的认证字符串
		String encodedCredentials = credentialXML.toString();

		// 调用content manager service的logon方法，此方法为stub的方法
		getCmService().logon(new XmlEncodedXML(encodedCredentials),
				new SearchPathSingleObject[] {});
	}

	public ContentManagerServiceStub getCmService() {
		return cmService;
	}

	public void setCmService(ContentManagerServiceStub cmService) {
		this.cmService = cmService;
	}

	public ContentManagerService_ServiceLocator getCmServiceLocator() {
		return cmServiceLocator;
	}

	public void setCmServiceLocator(
			ContentManagerService_ServiceLocator cmServiceLocator) {
		this.cmServiceLocator = cmServiceLocator;
	}

	public ReportService_ServiceLocator getReportServiceLocator() {
		return reportServiceLocator;
	}

	public void setReportServiceLocator(
			ReportService_ServiceLocator reportServiceLocator) {
		this.reportServiceLocator = reportServiceLocator;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}

}
