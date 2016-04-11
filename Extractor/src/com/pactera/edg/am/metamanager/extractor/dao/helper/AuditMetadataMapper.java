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

import com.pactera.edg.am.metamanager.extractor.bo.mm.AuditMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 审核元数据RowMapper
 *
 * @author hqchen
 * @version 1.0  Date: Oct 11, 2009
 */
public class AuditMetadataMapper implements RowMapper {

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
		AuditMetadata md = new AuditMetadata();
		md.setId(rs.getString("INSTANCE_ID"));
		md.setCode(rs.getString("INSTANCE_CODE"));
		md.setName(rs.getString("INSTANCE_NAME"));
		md.setClassifierId(rs.getString("CLASSIFIER_ID"));
		md.setNamespace(rs.getString("NAMESPACE"));
		md.setStartTime(rs.getLong("START_TIME"));
		md.setOperType(rs.getString("OPER_TYPE"));
		md.setDescription(rs.getString("DESCRIPTION"));
		
		MMMetadata parent = new MMMetadata();
		parent.setId(rs.getString("PARENT_ID"));
		md.setParentMetadata(parent);
		
		return md;
	}

}
