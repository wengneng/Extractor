package com.pactera.edg.am.metamanager.extractor.dao;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.ex.DataRollbackException;
import com.pactera.edg.am.metamanager.extractor.ex.MetadataNotFoundException;

/**
 * 元数据DAO
 * 批量入库策略:一种元模型的数据作为批量提交的单元,当一个单元中的记录数超过批量提交的记录数(由配置提供)时,将提交批量提交记录数的整数+1倍.
 * 批量修改策略同批量入库策略 批量删除策略:将需要删除的所有元数据ID记录入一条SQL中供一次删除(当记录数超过1000时,需作1000的整数倍+1次提交)
 * 
 * @author chenhanqing
 * @version 1.0 Date: Jul 7, 2009
 */
public interface IMetadataDao extends IDeleteDao {

	public static final String SPRING_NAME = "iMetadataDao";

	/**
	 * 根据元数据ID,从数据库表中获取其所属的元模型信息
	 * 
	 * @param metadataId
	 *            元数据ID
	 * @return 元模型
	 * @throws MetadataNotFoundException
	 */
	MMMetaModel queryMetaModelBySql(String metadataId) throws MetadataNotFoundException;

	/**
	 * 获取批量元数据的指定元模型的子结点列表
	 * 
	 * @param parentMetadatas
	 *            指定的父元数据列表
	 * @param metaModel
	 *            指定元模型
	 * @return 指定元模型的子结点列表
	 */
	List<MMMetadata> queryChildMetadatas(List<AbstractMetadata> parentMetadatas, MMMetaModel metaModel);

	/**
	 * 获取元数据的指定元模型的子结点列表
	 * 
	 * @param parentMetadatas
	 *            指定的父元数据
	 * @param metaModel
	 *            指定元模型
	 * @return 指定元模型的子结点列表
	 */
	List<AbstractMetadata> queryChildMetadatas(MMMetadata parentMetadata, MMMetaModel childMetaModel);

	/**
	 * 根据元数据ID及其元模型信息,获取该元数据详细信息
	 * 
	 * @param parentMetadata
	 *            元数据的父结点
	 * @param metadataId
	 *            元数据ID
	 * @param metaModel
	 *            元模型,需要根据元模型,获取该元数据有哪些属性,及都存在于哪些字段中
	 * @return 该元数据详细信息
	 */
	MMMetadata queryMetadata(MMMetadata parentMetadata, String metadataId, MMMetaModel metaModel);

	/**
	 * 判断该元数据结点是否已经存在于元数据库中
	 * 
	 * @param parentId
	 *            元数据的父结点ID
	 * @param metadata
	 *            待判断的元数据
	 * @return 如已经存在则返回true, 否则返回false
	 */
	boolean existMetadata(String parentId, MMMetadata metadata);

	/**
	 * 获取METADATAID的元数据
	 * 
	 * @param metadataId
	 */
	AppendMetadata queryAMetadata(String metadataId);

	/**
	 * 根据元数据命名空间,获取所有子孙元数据
	 * 
	 * @param aNamespace
	 *            元数据命名空间
	 * @return 子孙结点元数据列表
	 */
	List<MMMetadata> genChildrenMetadatasId(String aNamespace);

	/**
	 * 批量添加的元数据
	 * 
	 * @param createAMetadata
	 */
	void batchLoadCreate(AppendMetadata createAMetadata) throws Exception, DataRollbackException;

	/**
	 * 批量修改元数据
	 * 
	 * @param modifyAMetadata
	 */
	void batchLoadModify(AppendMetadata modifyAMetadata) throws Exception, DataRollbackException;

	/**
	 * 批量删除元数据
	 * 
	 * @param deleteAMetadata
	 */
	// void batchLoadDelete(AppendMetadata deleteAMetadata) throws Exception;
	void rollback(AppendMetadata createAMetadata) throws DataRollbackException;
	
	/**
	  * 根据当前节点ID获取父节点ID
	  * @Title: getParId  
	  * @Description: TODO 
	  * @param @param id
	  * @param @return 
	  * @return String 
	  * @throws
	 */
	String getParId(String id);

}
