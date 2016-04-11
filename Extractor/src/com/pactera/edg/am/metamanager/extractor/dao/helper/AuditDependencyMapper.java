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
package com.pactera.edg.am.metamanager.extractor.dao.helper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 审核依赖关系RowMapper
 *
 * @author hqchen
 * @version 1.0  Date: Oct 11, 2009
 */
public class AuditDependencyMapper implements RowMapper {

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
		AuditDependency aDependency = new AuditDependency();
		MMMetadata oMetadata = new MMMetadata();
		oMetadata.setId(rs.getString("FROM_INSTANCE_ID"));
		
		MMMetadata vMetadata = new MMMetadata();
		vMetadata.setId(rs.getString("TO_INSTANCE_ID"));
		
		aDependency.setCode(rs.getString("RELATIONSHIP"));
		aDependency.setType(rs.getString("OPER_TYPE"));
		
		return aDependency;
	}

}
