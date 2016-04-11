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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.db;

import java.sql.SQLException;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Catalog;

/**
 * DB适配器
 * 
 * @author user
 * @version 1.0 Date: Jul 23, 2009
 * 
 */
public interface IDBExtractService {

	/**
	 * @return
	 * @throws SQLException
	 */
	Catalog getCatalog() throws SQLException;

}
