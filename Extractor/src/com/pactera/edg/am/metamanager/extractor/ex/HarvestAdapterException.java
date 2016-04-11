package com.pactera.edg.am.metamanager.extractor.ex;

/**
 * 采集适配器异常。
 * 只要是属于适配器方面的异常，如RMI没有启动、RMI连接失败、发起采集的线程冲突、采集的资源不具备等。
 * 本类作为其他异常的父类，具体的异常应该继承本类。
 * @author fbchen
 * @version 1.0 2010-06-03
 */
public class HarvestAdapterException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public HarvestAdapterException() {
		super();
	}

	public HarvestAdapterException(String message, Throwable cause) {
		super(message, cause);
	}

	public HarvestAdapterException(String message) {
		super(message);
	}

	public HarvestAdapterException(Throwable cause) {
		super(cause);
	}

}
