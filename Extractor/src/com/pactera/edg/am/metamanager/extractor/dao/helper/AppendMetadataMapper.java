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

import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 悬挂结点RowMapper
 *
 * @author user
 * @version 1.0  Date: Oct 14, 2009
 *
 */
public class AppendMetadataMapper implements RowMapper {

	private String metadataId;

	public AppendMetadataMapper(String metadataId)
	{
		this.metadataId = metadataId;
	}

	public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
		AppendMetadata aMetadata = new AppendMetadata();
		MMMetadata metadata = new MMMetadata();
		metadata.setCode(rs.getString("INSTANCE_CODE"));
		metadata.setName(rs.getString("INSTANCE_NAME"));
		metadata.setClassifierId(rs.getString("CLASSIFIER_ID"));
		metadata.setNamespace(rs.getString("NAMESPACE"));
		metadata.setId(metadataId);
		metadata.setHasExist(true);
		aMetadata.setMetadata(metadata);

		MMMetaModel metaModel = new MMMetaModel();
		metaModel.setCode(metadata.getClassifierId());
		aMetadata.setMetaModel(metaModel);

		return aMetadata;
	}

}
