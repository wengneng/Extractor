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
 * 配置文件不存在的异常类
 * 
 * @author chenhq
 * @version 1.0 Date: Jul 10, 2009
 */
public class ConfFileNotFoundException extends HarvestAdapterException {
	private static final long serialVersionUID = 8025112156921251840L;

	public ConfFileNotFoundException() {
		super();
	}

	public ConfFileNotFoundException(String filePath) {
		super(new StringBuilder("配置文件[").append(filePath).append("]不存在!").toString());
	}

	public ConfFileNotFoundException(Throwable cause) {
		super(cause);
	}

	public ConfFileNotFoundException(String filePath, Throwable cause) {
		super(new StringBuilder("配置文件[").append(filePath).append("]不存在!").toString(), cause);
	}

}
