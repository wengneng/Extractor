/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.support.impl;

import com.pactera.edg.am.metamanager.core.common.connect.ConnectionVisitor;
import com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection;

/**
 * 测试记录采集的数据库连接是否正确
 * @author fbchen
 * @version 1.0 2010-07-23
 */
public class TestConnectDBData implements ITestConnection {
	
	private ConnectionVisitor connectionVisitor;

	public ConnectionVisitor getConnectionVisitor() {
		return connectionVisitor;
	}

	public void setConnectionVisitor(ConnectionVisitor connectionVisitor) {
		this.connectionVisitor = connectionVisitor;
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection#testConnect()
	 */
	public boolean testConnect() throws Exception {
		try {
			connectionVisitor.testConnection();
			this.testDialect();
		} finally {
			if (connectionVisitor != null) {
				connectionVisitor.destroy();
			}
		}
		return true;
	}
	
	/**
	 * 测试提供的数据库方言是否正确
	 * @return boolean
	 */
	private boolean testDialect() throws Exception {
		// TODO  connectionVisitor.getDialectClass();
		return true;
	}

}
