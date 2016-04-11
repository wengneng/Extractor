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
 * 内存溢出异常
 *
 * @author user
 * @version 1.0  Date: Aug 11, 2009
 *
 */
public class OutOfMemoryException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 348970930L;

	public OutOfMemoryException(String message)
	{
		super(message);
	}
	
	public OutOfMemoryException(Throwable cause)
	{
		super(cause);
	}

	public OutOfMemoryException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
