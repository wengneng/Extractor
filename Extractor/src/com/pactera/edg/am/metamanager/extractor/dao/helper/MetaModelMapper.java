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
import java.util.List;

import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.extractor.bo.composite.MAttribute;

/**
 * 设置元模型的属性在元数据表中的存储位置
 * 
 * @author user
 * @version 1.0 Date: Jul 9, 2009
 * 
 */
public class MetaModelMapper implements RowMapper {

	private List<MAttribute> attrs;

	public MetaModelMapper(List<MAttribute> attrs)
	{
		this.attrs = attrs;
	}

	public Object mapRow(ResultSet arg0, int arg1) throws SQLException {
		// 取属性名称
		String attrName = arg0.getString("ATT_NAME");

		Iterator<MAttribute> attrsIter = attrs.iterator();
		while (attrsIter.hasNext()) {
			MAttribute mAttribute = attrsIter.next();
			if (mAttribute.getName().equals(attrName)) {
				// 如有这个属性，则将其存放的位置存储于此
				mAttribute.setStorePosition(arg0.getString("ATT_STORE"));
				// 此处可能需要变更
				mAttribute.setDataType(arg0.getInt("DATATYPE"));
			}
		}
		return null;
	}

}
