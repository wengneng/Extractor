/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.support.impl;

import java.sql.Connection;

import javax.sql.DataSource;

import com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection;

/**
 * 测试数据库连接是否正确
 * @author fbchen
 * @version 1.0 2010-07-23
 */
public class TestConnectDB implements ITestConnection {
	
	private DataSource dataSource;

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection#testConnect()
	 */
	public boolean testConnect() throws Exception {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		return true;
	}

}
