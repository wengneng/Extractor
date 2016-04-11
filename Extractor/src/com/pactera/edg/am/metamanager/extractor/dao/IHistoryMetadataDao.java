package com.pactera.edg.am.metamanager.extractor.dao;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;

/**
 * 历史元数据DAO
 * 批量入库策略:一种元模型的数据作为批量提交的单元,当一个单元中的记录数超过批量提交的记录数(由配置提供)时,将提交批量提交记录数的整数+1倍.
 * 批量修改策略同批量入库策略 批量删除策略:将需要删除的所有元数据ID记录入一条SQL中供一次删除(当记录数超过1000时,需作1000的整数倍+1次提交)
 * 
 * @author user
 * @version 1.0 Date: Jul 7, 2009
 * 
 */
public interface IHistoryMetadataDao extends IRollbackDao{
	
	public static final String SPRING_NAME = "iHistoryMetadataDao";

	/**
	 * 批量记录需要删除,修改的元数据,至历史元数据表
	 * 
	 * @param aMetadata
	 *            需要删除/修改的元数据对象
	 * @param type 元数据类型:删除:D;修改:M
	 */

	void batchLoadCreate(AppendMetadata aMetadata, String type);

}
