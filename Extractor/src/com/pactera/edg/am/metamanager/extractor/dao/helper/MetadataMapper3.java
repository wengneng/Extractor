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
import java.util.Iterator;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 元数据RM映射类
 * 
 * @author hqchen
 * @version 1.0 Date: Aug 21, 2009
 */
public class MetadataMapper3 implements RowMapper {

	private MMMetaModel metaModel;

	public MetadataMapper3(MMMetaModel metaModel) {
		this.metaModel = metaModel;
	}

	public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
		MMMetadata metadata = new MMMetadata();
		metadata.setClassifierId(metaModel.getCode());
		metadata.setId(rs.getString(1));
		metadata.setHasExist(true);
		metadata.setCode(rs.getString(2));
		metadata.setName(rs.getString(3));
		metadata.setNamespace(rs.getString(4));
		Map<String, String> mAttributes = metaModel.getMAttrs();
		for (Iterator<String> iter = mAttributes.keySet().iterator(); iter.hasNext();) {
			// 遍历设置属性
			String key = iter.next();
			// 数据类型为varchar
			String value = rs.getString(mAttributes.get(key));
			if (value != null) {
				metadata.addAttr(key, value);
			}
		}
		return metadata;
	}
}
