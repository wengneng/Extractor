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
 * 元数据不存在于数据库中的异常类
 *
 * @author user
 * @version 1.0  Date: Aug 9, 2009
 *
 */
public class MetadataNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 355789387409824L;
	
	/**
	 * @param message
	 */
	public MetadataNotFoundException(String metadataPath)
	{
		super("元数据[" + metadataPath + "]不存在!");
	}

	/**
	 * @param cause
	 */
	public MetadataNotFoundException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public MetadataNotFoundException(String metadataPath, Throwable cause)
	{
		super("元数据[" + metadataPath + "]不存在!", cause);
	}

}
