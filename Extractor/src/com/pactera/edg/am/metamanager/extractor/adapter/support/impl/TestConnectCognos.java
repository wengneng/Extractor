/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.support.impl;

import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl.CognosConnector83;
import com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection;
import com.pactera.edg.am.metamanager.extractor.adapter.support.TestConnectionException;

/**
 * 测试连接Cognose服务器（报表）
 * @author fbchen
 * @version 1.0 2010-07-23
 */
public class TestConnectCognos implements ITestConnection {
	
	private CognosConnector83 cognosConnector;

	public CognosConnector83 getCognosConnector() {
		return cognosConnector;
	}
	
	public void setCognosConnector(CognosConnector83 cognosConnector) {
		this.cognosConnector = cognosConnector;
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection#testConnect()
	 */
	public boolean testConnect() throws Exception {
		ContentManagerService_Port cms = null;
		try {
			cms = cognosConnector.connectByWebService();
			if (cms == null) {
				throw new TestConnectionException("连接Cognos服务器失败");
			}
		} finally {
			cognosConnector.logOff();
		}
		return true;
	}

}
