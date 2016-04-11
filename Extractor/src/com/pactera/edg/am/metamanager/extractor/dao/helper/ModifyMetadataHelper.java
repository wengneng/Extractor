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
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.ModifyAttribute;
import com.pactera.edg.am.metamanager.extractor.bo.mm.ModifyMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 批量更新元数据操作的PreparedStatementCallback辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Aug 21, 2009
 */
public class ModifyMetadataHelper extends PreparedStatementCallbackHelper {

	public ModifyMetadataHelper(int batchSize)
	{
		super(batchSize);
	}

	private MMMetaModel metaModel;

	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException {
		// 从元模型中获取属性

		Map<String, String> mAttrs = metaModel.getMAttrs();

		// 从元模型中获取属性该元数据的所有元数据
		List<AbstractMetadata> metadatas = metaModel.getMetadatas();
		int size = metadatas.size(), mSize = mAttrs.size();
		long cntTime = AdapterExtractorContext.getInstance().getGlobalTime();
		// sequence
		for (int i = 0; i < size; i++) {

			ModifyMetadata metadata = (ModifyMetadata) metadatas.get(i);

			ps.setString(1, metadata.getName());
			ps.setLong(2, cntTime);
			// 元数据属性
			Map<String, String> updateAttrs = genUpdateAttrs(metadata, mAttrs);
			// 设置元数据的属性
			setAttrs(mAttrs, updateAttrs, ps);
			// 元数据ID
			setPs(ps, mSize + 2, metadata.getId());

			ps.addBatch();
			ps.clearParameters();
			if (++super.count % super.batchSize == 0) {
				ps.executeBatch();
				ps.clearBatch();
			}
		}
		if (super.count % super.batchSize != 0) {
			ps.executeBatch();
			ps.clearBatch();

		}

		return null;
	}

	protected void setPs(PreparedStatement ps, int index, String id) throws SQLException {
		ps.setString(index + 1, id);

	}

	/**
	 * 获取需要更新的元数据属性
	 * 
	 * @param metadata
	 * @param attrs
	 * @param ps
	 * @return
	 */
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

	private void setAttrs(Map<String, String> mAttrs, Map<String, String> updateAttrs, PreparedStatement ps)
			throws SQLException {
		int index = 2;
		for (Iterator<String> iter = mAttrs.keySet().iterator(); iter.hasNext();) {
			String mAttrName = iter.next();
			if (updateAttrs.containsKey(mAttrName)) {
				// 该属性有设置值,则设置它的值
				String val = updateAttrs.get(mAttrName) == null ? "" : updateAttrs.get(mAttrName);
				ps.setString(index + 1, val);
			}
			else {
				// 否则设置该值为NULL
				ps.setNull(index + 1, Types.VARCHAR);
			}
			index++;
		}

	}

	public void setMetaModel(MMMetaModel metaModel) {
		this.metaModel = metaModel;
	}
}
