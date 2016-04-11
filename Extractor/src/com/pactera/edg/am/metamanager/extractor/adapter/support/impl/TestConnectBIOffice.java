/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.support.impl;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.webService.BofConnector;
import com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection;
import com.pactera.edg.am.metamanager.extractor.adapter.support.TestConnectionException;

/**
 * 测试连接BI-Office服务器（报表）
 * 
 * @author fbchen
 * @version 1.0 2010-07-23
 */
public class TestConnectBIOffice implements ITestConnection {

	private BofConnector bofConnector;

	public BofConnector getBofConnector() {
		return bofConnector;
	}

	public void setBofConnector(BofConnector bofConnector) {
		this.bofConnector = bofConnector;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection#testConnect()
	 */
	public boolean testConnect() throws Exception {
		boolean isconn = false;
		isconn = bofConnector.login();
		if (isconn == false) {
			throw new TestConnectionException("连接BI.Office服务器失败");
		}
		return true;
	}

}
