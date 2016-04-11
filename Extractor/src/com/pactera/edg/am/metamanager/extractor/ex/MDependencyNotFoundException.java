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
 * 不存在依赖关系的异常
 * 
 * @author user
 * @version 1.0 Date: Oct 6, 2009
 * 
 */
public class MDependencyNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 38092380137L;

	public MDependencyNotFoundException()
	{
		super();
	}

	/**
	 * @param message
	 */
	public MDependencyNotFoundException(String errorDependencies)
	{
		super("如下元模型的依赖关系不存在:" + errorDependencies);
	}

	/**
	 * @param cause
	 */
	public MDependencyNotFoundException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public MDependencyNotFoundException(String errorDependencies, Throwable cause)
	{
		super("如下元模型的依赖关系不存在,CODE为空:" + errorDependencies);
	}
}
