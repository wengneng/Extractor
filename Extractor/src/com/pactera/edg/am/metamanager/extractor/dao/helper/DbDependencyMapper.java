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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 数据依赖关系RowMapper
 * 
 * @author qhchen
 * @version 1.0 Date: Sep 25, 2009
 */
public class DbDependencyMapper implements RowMapper {

	public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
		MMDDependency dbDependency = new MMDDependency();
		MMMetadata omd = new MMMetadata();
		omd.setId(rs.getString("FROM_INSTANCE_ID"));
		omd.setClassifierId(rs.getString("FROM_CLASSIFIER_ID"));
		
		MMMetadata vmd = new MMMetadata();
		vmd.setId(rs.getString("TO_INSTANCE_ID"));
		vmd.setClassifierId(rs.getString("TO_CLASSIFIER_ID"));
		
		dbDependency.setOwnerMetadata(omd);
		dbDependency.setValueMetadata(vmd);
		dbDependency.setCode(rs.getString("RELATIONSHIP"));
		
		return dbDependency;
	}

}
