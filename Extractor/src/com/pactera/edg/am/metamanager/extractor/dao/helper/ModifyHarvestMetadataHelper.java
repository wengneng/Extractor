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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.ModifyAttribute;
import com.pactera.edg.am.metamanager.extractor.bo.mm.ModifyMetadata;

/**
 * 操作审核元数据表时,对需修改的元数据操作的PreparedStatementCallback辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Oct 5, 2009
 */
public class ModifyHarvestMetadataHelper extends CreateHarvestMetadataHelper {

	private Log log = LogFactory.getLog(CreateMetadataHelper.class);

	public ModifyHarvestMetadataHelper(int batchSize) {
		super(batchSize);
	}
	
	protected void setPs(PreparedStatement ps, MMMetadata metadata, int index) throws SQLException {
		// START_TIME: 旧元数据的start_time
		ps.setLong(7, metadata.getStartTime());
		
		// 任务实例ID
		ps.setString(index + 1, taskInstanceId);
		// 表示待审核
		ps.setString(index + 2, "0");
		// 表示操作的类型:增加,修改,删除
		ps.setString(index + 3, operType);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.dao.helper.CreateHarvestMetadataHelper
	 * #getMetadataAttrPrepared(com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata)
	 */
	protected Map<String, String> getMetadataAttrPrepared(MMMetadata metadata) {
		ModifyMetadata mMetadata = (ModifyMetadata) metadata;
		Map<String, String> mAttrs = metaModel.getMAttrs();
		return genUpdateAttrs(mMetadata, mAttrs);
	}

	private Map<String, String> genUpdateAttrs(ModifyMetadata metadata, Map<String, String> attrs) {
		Map<Operation, Map<String, String>> modifyAttrs = metadata.getModifyAttrs();
		// 需要更新的元数据属性:包括添加的,修改的,不变的元数据属性
		Map<String, String> updateAttrs = new HashMap<String, String>();

		Map<String, String> createAttrs = modifyAttrs.get(Operation.CREATE);

		if (createAttrs != null) {
			updateAttrs.putAll(createAttrs);
		}
		Map<String, String> changelessAttrs = modifyAttrs.get(Operation.CHANGELESS);
		if (changelessAttrs != null) {
			updateAttrs.putAll(changelessAttrs);
		}

		List<ModifyAttribute> changeAttrs = metadata.getMAttrs();
		if (changeAttrs != null) {
			updateAttrs.putAll(genChangeAttrs(changeAttrs));
		}
		return updateAttrs;

	}

	private Map<String, String> genChangeAttrs(List<ModifyAttribute> changeAttrs) {
		Map<String, String> modifyAttrs = new HashMap<String, String>(changeAttrs.size());
		for (ModifyAttribute changeAttr : changeAttrs) {
			modifyAttrs.put(changeAttr.getKey(), changeAttr.getNewValue());
		}
		return modifyAttrs;
	}

}