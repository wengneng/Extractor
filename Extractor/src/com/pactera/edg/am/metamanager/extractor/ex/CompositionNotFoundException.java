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
 * 两元数据之间不存在组合关系的异常
 * 
 * @author user
 * @version 1.0 Date: Aug 11, 2009
 * 
 */
public class CompositionNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 759387580980234L;

	public CompositionNotFoundException(String msg)
	{
		super(msg);
	}

	public CompositionNotFoundException(String parentCode, String code)
	{
		super(new StringBuilder("元模型[").append(parentCode).append("]与元模型[").append(code).append(
				"]之间不存在组合关系,请检查数据是否正常,或库表中是否存在组合关系!").toString());
	}

	public CompositionNotFoundException(Throwable cause)
	{
		super(cause);
	}

	public CompositionNotFoundException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
