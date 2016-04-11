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

package com.pactera.edg.am.metamanager.extractor.dao.helper;

import org.springframework.jdbc.core.PreparedStatementCallback;

/**
 * PreparedStatementCallback接口的实现抽象类
 * 
 * @author user
 * @version 1.0 Date: Aug 21, 2009
 * 
 */
public abstract class PreparedStatementCallbackHelper implements PreparedStatementCallback {

	/**
	 * 一次批量提交数据的最大记录数
	 */
	protected int batchSize;

	// 提交数据的总数据量
	protected int count;

	public PreparedStatementCallbackHelper(int batchSize)
	{
		this.batchSize = batchSize;
	}

	public int getCount() {
		return count;
	}
}
