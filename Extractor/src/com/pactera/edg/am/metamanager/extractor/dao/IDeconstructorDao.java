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

import java.util.List;
import java.util.Map;

public interface IDeconstructorDao {

	void deconstrunctor();

	void deleteData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas);
	
	void deleteMappingData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas);

}
