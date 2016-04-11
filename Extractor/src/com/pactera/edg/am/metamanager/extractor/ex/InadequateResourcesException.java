/*
 * Copyright 2009 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, BeiJing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

package com.pactera.edg.am.metamanager.extractor.ex;

/**
 * 运行采集所需的资源不足，一般一次只允许一个采集线程执行采集，其他采集线程需等待资源的释放。
 * 
 * @author fbchen
 * @version 1.0 2010-06-03
 */
public class InadequateResourcesException extends HarvestAdapterException {
	private static final long serialVersionUID = 8025112156921251840L;

	public InadequateResourcesException() {
		super();
	}

	public InadequateResourcesException(String msg) {
		super(msg);
	}

	public InadequateResourcesException(Throwable cause) {
		super(cause);
	}

	public InadequateResourcesException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
