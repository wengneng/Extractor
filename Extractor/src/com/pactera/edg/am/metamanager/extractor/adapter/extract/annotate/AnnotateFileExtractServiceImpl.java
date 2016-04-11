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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.annotate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.ICommonExtractService;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * (简单概要描述此类完成的功能)
 * 
 * @author user
 * @version 1.0 Date: Nov 9, 2009
 * 
 */
public class AnnotateFileExtractServiceImpl implements ICommonExtractService {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.ICommonExtractService#extract()
	 */
	public InputStream extract() {
		String path = AdapterExtractorContext.getInstance().getDsAbsolutePath();
		try {
			return new FileInputStream(new File(path));
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

}
