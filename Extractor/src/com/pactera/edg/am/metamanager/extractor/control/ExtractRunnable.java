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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 采集线程
 * 
 * @author user
 * @version 1.0 Date: Mar 24, 2010
 * 
 */
public class ExtractRunnable implements Runnable {

	private String taskInstanceId;

	public ExtractRunnable(String taskInstanceId)
	{
		this.taskInstanceId = taskInstanceId;
	}

	public void run() {
		new ExtractorController().adapterControl(taskInstanceId);
	}

	public static void main(String[] args) {
		Thread t = new Thread(new ExtractRunnable("3l"));
		ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
		pool.execute(t);
		pool.shutdown();

	}

}
