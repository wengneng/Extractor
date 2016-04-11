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

package com.pactera.edg.am.metamanager.extractor.log;

import java.util.Observable;
import java.util.Observer;

import com.pactera.edg.am.metamanager.extractor.dao.IExtractorLogDao;

/**
 * 采集日志主题
 * 
 * @author user
 * @version 1.0 Date: Oct 8, 2009
 * 
 */
public class ExtractorLogObserver implements Observer {

	// private Log log = LogFactory.getLog(ExtractorLogObserver.class);
	//	

	/**
	 * 采集日志观察者
	 */
	private Observable observable;

	/**
	 * 写日志DAO接口
	 */
	private IExtractorLogDao extractorLogDao;

	public ExtractorLogObserver(Observable observable)
	{
		// 注册主题至观察者
		this.observable = observable;
		this.observable.addObserver(this);
	}

	/**
	 * 批量写日志至库表中
	 * 
	 * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
	 */
	public void update(Observable o, Object arg) {
		if (o instanceof ExtractorLogObservable) {
			ExtractorLogObservable eObservable = (ExtractorLogObservable) o;
			extractorLogDao.batchCreate(eObservable.getExtractorLogs());
		}

	}

	public void setExtractorLogDao(IExtractorLogDao extractorLogDao) {
		this.extractorLogDao = extractorLogDao;
	}

	public void setObservable(Observable observable) {
		this.observable = observable;
	}

}
