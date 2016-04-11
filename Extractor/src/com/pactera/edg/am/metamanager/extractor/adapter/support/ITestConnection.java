/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.support;

/**
 * 提供测试连接的方法，以校验相关的配置信息是否正确。
 * @author fbchen
 * @version 1.0 2010-07-23
 */
public interface ITestConnection {

	/**
	 * 测试连接
	 * @return Boolean true-成功
	 * @exception Exception 连接失败，Exception往上抛
	 */
	boolean testConnect() throws Exception;
	
}
