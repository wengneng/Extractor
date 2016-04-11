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

package com.pactera.edg.am.metamanager.extractor.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.pactera.edg.am.metamanager.app.bo.Attribute;
import com.pactera.edg.am.metamanager.app.bo.Classifier;
import com.pactera.edg.am.metamanager.app.bo.CompRelation;
import com.pactera.edg.am.metamanager.app.bo.DepRelation;
import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;
import com.pactera.edg.am.metamanager.extractor.bo.composite.MetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IMetaModelDao;
import com.pactera.edg.am.metamanager.extractor.dao.helper.MetaModelMapper;
import com.pactera.edg.am.metamanager.extractor.ex.CompositionNotFoundException;
import com.pactera.edg.am.metamanager.extractor.ex.MetaModelNotFoundException;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 元模型操作实现类:分手写SQL操作方式,RMI调用API接口方式
 * 
 * @author user
 * @version 1.0 Date: Jul 9, 2009
 * 
 */
public class MetaModelDaoImpl extends DaoBaseServiceImpl implements IMetaModelDao {

	private Log log = LogFactory.getLog(MetaModelDaoImpl.class);

	/**
	 * 设置父子两模型间的组合关系代码
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IMetaModelDao#setCompedRelation(com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel,
	 *      com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel)
	 */
	public void setCompedRelation(MMMetaModel parentMetaModel, MMMetaModel metaModel)
			throws CompositionNotFoundException {

		CompRelation cr = null;
		if (parentMetaModel == null) {
			metaModel.setCompedRelationCode("##");
		}
		else {
			IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance().getIClassifier();

			cr = iClassifier.getCompRelation(parentMetaModel.getCode(), metaModel.getCode(), true);
			if (cr == null) { throw new CompositionNotFoundException(parentMetaModel.getCode(), metaModel.getCode()); }
			metaModel.setCompedRelationCode(cr.getId());
		}

	}

	/**
	 * RMI调用API接口方式
	 * 
	 * @param metaModel
	 * @throws MetaModelNotFoundException
	 */
	public void genStorePositionByApi(MMMetaModel metaModel) throws MetaModelNotFoundException {
		// 从spring容器中获取元模型获取接口
		IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance().getIClassifier();

		Classifier classifier = iClassifier.getClassifier(metaModel.getCode());
		if (classifier == null) { throw new MetaModelNotFoundException(metaModel.getCode()); }
		metaModel.setMAttrs(getMAttrs(classifier));
	}

	public void genMAttr(MMMetaModel metaModel, String attr) {

	}

	/**
	 * 根据元模型ID获取所有子元模型
	 * 
	 * @param metaModelCode
	 * @return
	 */
	public List<MMMetaModel> queryChileMetaModel(final String metaModelCode) {
		IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance().getIClassifier();

		List<Classifier> childClassifiers = iClassifier.listCompClassifiers(metaModelCode);

		if (childClassifiers == null || childClassifiers.size() == 0) {
			// 子结点为空
			return Collections.emptyList();
		}
		List<MMMetaModel> childMetaModels = new ArrayList<MMMetaModel>();
		for (int i = 0, size = childClassifiers.size(); i < size; i++) {
			MMMetaModel childMetaModel = new MMMetaModel();
			Classifier childClassifier = childClassifiers.get(i);
			childMetaModel.setCode(childClassifier.getId());

			childMetaModel.setName(childClassifier.getName());
			// 设置模型的属性
			childMetaModel.setMAttrs(getMAttrs(childClassifier));

			childMetaModels.add(childMetaModel);
		}

		return childMetaModels;
	}

	/**
	 * 对模型的属性的转换
	 * 
	 * @param classifier
	 * @return
	 */
	private Map<String, String> getMAttrs(Classifier classifier) {
		List<Attribute> attributeList = classifier.getAttributes();
		if (attributeList == null || attributeList.size() == 0) { return Collections.emptyMap(); }
		Iterator<Attribute> attributes = attributeList.iterator();

		Map<String, String> mAttrs = new HashMap<String, String>(attributeList.size());

		while (attributes.hasNext()) {
			Attribute attr = attributes.next();

			// 元模型属性的code标识了该属性
			mAttrs.put(attr.getCode(), attr.getStoreColumn());

			// String storeColumn = attr.getStoreColumn().toUpperCase();
			// if (storeColumn.startsWith("STRING")) {
			// // 如开始于string则表示存储于varchar2类型中
			// mAttribute.setDataType(Types.VARCHAR);
			// }
			// else {
			// // 否则表示存储于clob类型中
			// mAttribute.setDataType(Types.CLOB);
			// }
			// // 设置元模型属性的存储位置
			// mAttribute.setStorePosition(attr.getStoreColumn());
			// mAttrs.add(mAttribute);
		}

		return mAttrs;
	}

	/**
	 * 手写SQL方式
	 */
	public void genStorePosition(MetaModel metaModel) {
		String sql = super.getSql("METAMODEL_ATTR_STORE");

		try {
			getJdbcTemplate().queryForObject(sql, new Object[] { metaModel.getName() },
					new MetaModelMapper(metaModel.getAttrs()));
		}
		catch (EmptyResultDataAccessException e) {
			e.printStackTrace();
		}
	}

	public String genRelationCode(MMDDependency dependency) {
		IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance().getIClassifier();
		try {
			DepRelation depRelation = iClassifier.getDepRelation(dependency.getOwnerMetadata().getClassifierId(),
					dependency.getOwnerRole(), dependency.getValueMetadata().getClassifierId(), dependency
							.getValueRole());
			if (depRelation == null)
				return null;

			return depRelation.getId();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void genAttrs(MMMetaModel metaModel) {
		String sql = super.getSql("QUERY_METAMODEL_ATTRS");

		final Map<String, String> mAttrs = new HashMap<String, String>();
		super.getJdbcTemplate().query(sql, new Object[] { metaModel.getCode() }, new RowCallbackHandler() {

			public void processRow(ResultSet rs) throws SQLException {
				mAttrs.put(rs.getString("ATT_CODE"), rs.getString("ATT_STORE"));

			}
		});

		metaModel.setMAttrs(mAttrs);
	}

}
