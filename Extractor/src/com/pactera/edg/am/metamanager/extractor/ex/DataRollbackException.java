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
 * 回滚数据异常类
 *
 * @author user
 * @version 1.0  Date: Mar 2, 2011
 *
 */
public class DataRollbackException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 35578938736665L;
	
	/**
	 * @param message
	 */
	public DataRollbackException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public DataRollbackException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public DataRollbackException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
