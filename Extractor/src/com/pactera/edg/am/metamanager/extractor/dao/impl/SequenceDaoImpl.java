/*
 * Copyright 2011 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, BeiJing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

package com.pactera.edg.am.metamanager.extractor.dao.impl;

import java.util.UUID;

import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.pactera.edg.am.metamanager.core.components.uuid.FastUUIDGen;
import com.pactera.edg.am.metamanager.core.components.uuid.UUIDGen;
import com.pactera.edg.am.metamanager.core.components.uuid.UUIDGenFactory;
import com.pactera.edg.am.metamanager.extractor.dao.ISequenceDao;

/**
 * 库表的sequence操作,包括查询,自加
 * 
 * @author hqchen
 * @version 1.0 Date: Jul 23, 2009
 * @author fbchen [add uuid gen method] 2010-07-30
 */
public class SequenceDaoImpl extends JdbcDaoSupport implements ISequenceDao {

	private static final String SEQUENCE_FLAG = "SEQ_";
	
	private static final Byte LOCK = 'L';
	
	private UUIDGen uuidGen;

	public SequenceDaoImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.ISequenceDao#getNextval(java.lang.String)
	 */
	public long getNextval(String tableName) {
		String sequenceName = getSequenceName(tableName);
		return super.getJdbcTemplate().queryForLong(
				new StringBuilder("select ").append(sequenceName).append(".nextval from dual").toString());
	}

	private String getSequenceName(String tableName) {
		return (SEQUENCE_FLAG + tableName).toUpperCase();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.ISequenceDao#getUuid()
	 */
	public String getUuid() {
		StringBuffer uuid = new StringBuffer();
		try {
			uuid.append(UUID.randomUUID().toString());
		} catch (Exception e) {
			this.createUUIDGen();
			uuid.append(uuidGen.nextUUID());
		}
		if (uuid.length() == 0) {
			throw new RuntimeException("UUID generate fail");
		}
		uuid.setCharAt(0, shiftHex(uuid.charAt(0))); //以便区别于hibernate的uuid.hex生成方式
		return uuid.toString().replaceAll("-", "");
	}
	
	/**
	 * 将16进制的字符转换成非16进制的英文字母，如0~9 =&gt; g~p，a~f =&gt; q~v，A~F =&gt; Q~V
	 * @param c 被转换的16进制字符
	 * @return 转换后的字符
	 */
	private static char shiftHex(char c) {
		return (char)(c + (c<='9' ? 55 : 16));
	}
	
	/**
	 * 创建UUID生成器，通过控制线程同步只生成一个生成器<br>
	 * 可选的生成器有：<ul>
	 * <li>com.pactera.edg.am.metamanager.core.components.uuid.SimpleUUIDGen - 生成速度较慢</li>
	 * <li>com.pactera.edg.am.metamanager.core.components.uuid.FastUUIDGen - 生成速度较快</li>
	 * </ul>
	 */
	public void createUUIDGen() {
		if (uuidGen == null) {
			synchronized (LOCK) {
				if (uuidGen == null) {
					uuidGen = UUIDGenFactory.getUUIDGen(FastUUIDGen.class);
				}
			}
		}
	}
	
}
