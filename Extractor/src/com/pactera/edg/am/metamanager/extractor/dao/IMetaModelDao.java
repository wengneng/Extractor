package com.pactera.edg.am.metamanager.extractor.dao;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.composite.MetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.ex.CompositionNotFoundException;
import com.pactera.edg.am.metamanager.extractor.ex.MetaModelNotFoundException;

/**
 * 元模型DAO
 * 
 * @author user
 * @version 1.0 Date: Jul 7, 2009
 * 
 */
public interface IMetaModelDao {

	/**
	 * 通过查询数据库表,获取指定元模型的属性在元数据表中的存储位置
	 * 
	 * @param metaModel
	 */
	void genStorePosition(MetaModel metaModel);

	/**
	 * 通过RMI访问机制,获取指定元模型的属性在元数据表中的存储位置
	 * 
	 * @param metaModel
	 */
	void genStorePositionByApi(MMMetaModel metaModel) throws MetaModelNotFoundException;

	/**
	 * 根据元模型CODE,获取其所有子模型
	 * 
	 * @param metaModelCode
	 * @return
	 */
	public List<MMMetaModel> queryChileMetaModel(final String metaModelCode);

	/**
	 * 设置parentMetaModel与metaModel之间的组合关系CODE
	 * 
	 * @param parentMetaModel
	 * @param metaModel
	 */
	void setCompedRelation(MMMetaModel parentMetaModel, MMMetaModel metaModel) throws CompositionNotFoundException;

	/**
	 * 根据依赖关系的ownerClass,ownerRole,valueClass,valueRole,获取该依赖关系的CODE
	 * 
	 * @param dependency
	 * @return
	 */
	String genRelationCode(MMDDependency dependency);

	void genAttrs(MMMetaModel columnMetaModel);

}
