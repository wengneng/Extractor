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

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 元模型不存在于数据库表的异常
 * 
 * @author user
 * @version 1.0 Date: Aug 1, 2009
 * 
 */
public class MetaModelNotFoundException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3464767878978L;

	public MetaModelNotFoundException(String metaModelId)
	{
		super(new StringBuilder("不存在CODE为[").append(metaModelId).append("]的元模型!").toString());

		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, new StringBuilder("不存在CODE为[").append(
				metaModelId).append("]的元模型!").toString());
	}

	public MetaModelNotFoundException(Throwable cause)
	{
		super(cause);
	}

	public MetaModelNotFoundException(String metaModelId, Throwable cause)
	{
		super("不存在ID为[" + metaModelId + "]的元模型!", cause);
	}

}
