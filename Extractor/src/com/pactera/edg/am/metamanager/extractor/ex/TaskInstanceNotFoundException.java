/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, BeiJing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */


package com.pactera.edg.am.metamanager.extractor.ex;

/**
 * 没有任务实例信息异常
 *
 * @author hqchen
 * @version 1.0  Date: Aug 11, 2009
 */
public class TaskInstanceNotFoundException extends Exception {

	private static final long serialVersionUID = 1464657687987L;

	public TaskInstanceNotFoundException(String message) {
		super(message);
	}
	
	public TaskInstanceNotFoundException(Throwable cause) {
		super(cause);
	}

	public TaskInstanceNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}
}
