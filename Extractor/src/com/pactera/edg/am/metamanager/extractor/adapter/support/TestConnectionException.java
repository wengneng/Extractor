/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.support;

/**
 * 测试连接异常
 * @author fbchen
 * @version 1.0 2010-07-23
 */
public class TestConnectionException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TestConnectionException() {
		super();
	}

	public TestConnectionException(String message, Throwable cause) {
		super(message, cause);
	}

	public TestConnectionException(String message) {
		super(message);
	}

	public TestConnectionException(Throwable cause) {
		super(cause);
	}

}
