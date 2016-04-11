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

package com.pactera.edg.am.metamanager.extractor.dao;

/**
 * sequence操作的DAO
 *
 * @author hqchen
 * @version 1.0  Date: Jul 23, 2009
 */
public interface ISequenceDao {

	/**
	 * 返回指定表的下一个SEQUENCE值。<br>
	 * 考虑到系统在不同数据库上的移植问题，部分数据库不支持SEQUENCE，因此不推荐使用该方式生成ID。
	 * @param tableName 表名
	 * @return 返回SEQUENCE值
	 */
	long getNextval(String tableName);
	
	/**
	 * 生成一个32位长度的UUID
	 * @return str uuid
	 */
	String getUuid();
	
}
