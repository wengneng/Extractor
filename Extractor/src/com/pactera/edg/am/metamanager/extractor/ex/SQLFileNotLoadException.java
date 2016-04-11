/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.ex;

/**
 * SQL 文件不能加载异常
 * @author fbchen
 * @version 1.0 2010-05-18
 */
public class SQLFileNotLoadException extends RuntimeException {
	private static final long serialVersionUID = 9025936906906275111L;

	public SQLFileNotLoadException() {
		super();
	}

	public SQLFileNotLoadException(String message, Throwable cause) {
		super(message, cause);
	}

	public SQLFileNotLoadException(String message) {
		super(message);
	}

	public SQLFileNotLoadException(Throwable cause) {
		super(cause);
	}

}
