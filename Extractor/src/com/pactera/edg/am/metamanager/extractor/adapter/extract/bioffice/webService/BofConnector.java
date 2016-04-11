package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.webService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.sdk.ClientConnector;
import bof.sdk.RemoteException;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl.ParseModel;

/**
 * bi.office 连接类
 * 
 * @author 游林杰
 * 
 */
public class BofConnector {
	private Log log = LogFactory.getLog(ParseModel.class);
	private String ip;
	private String port;
	private String userName;
	private String password;
	private final int limitCount = 60;
	private int limit = 0;
	private ClientConnector cc;

	public boolean login() {
		cc = getConn();
		if (cc != null) {
			return true;
		}
		return false;
	}

	public ClientConnector getConn() {
		String biofficeURL = "http://" + ip + ":" + port + "/bioffice";
		ClientConnector cc = new ClientConnector(biofficeURL);
		try {
			boolean isConn = cc.open(userName, password);
			if (isConn) {
				// log.info("登录bi.office成功");
				return cc;
			}
		} catch (RemoteException e) {
			log.info("登录bi.office失败");
			e.printStackTrace();
		}
		log.info("登录bi.office失败");
		return null;
	}

	/**
	 * 获取连接 控制调用次数
	 * 
	 * @return
	 */
	public ClientConnector getConnByLimitMethod() {
		limit++;

		// 如果超过限制的调用次数
		if (limit > limitCount) {
			// 置为1
			limit = 1;
			this.closeConn();
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// 新建连接
			this.login();
		} else {
			if (cc == null) {
				// 新建连接
				this.login();
			}
		}
		return cc;
	}

	public void closeConn() {
		if (cc != null) {
			this.cc.close();
		}
		limit = 0;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
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
}
