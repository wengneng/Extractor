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

package com.pactera.edg.am.metamanager.extractor.control;

/**
 * 采集服务接口
 * 
 * @author user
 * @version 1.0 Date: Sep 27, 2009
 * 
 */
public interface IExtractorService {

	/**
	 * 采集数据，增量分析，入库
	 * @throws Throwable
	 */
	public void extract() throws Throwable;
}
