package com.pactera.edg.am.metamanager.extractor.dao;

import java.util.Collection;
import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 依赖关系DAO 包括批量添加,修改,删除元数据依赖关系
 * 批量添加依赖关系:遍历依赖关系,并将记录都添加进PreparedStatement中,当记录数等于批量提交记录数的倍数时,将提交PreparedStatement,同时清除其中记录;最后批量提交记录尾数
 * 批量删除依赖关系同批量添加依赖关系
 * 
 * @author user
 * @version 1.0 Date: Jul 7, 2009
 * 
 */
public interface IDependencyDao extends ICreateDao, IDeleteDao {

	/**
	 * 根据元数据的命名空间,获取该元数据的所有子孙结点的依赖关系
	 * 
	 * @param namespace
	 *            元数据的命名空间
	 * @return 所有所有元数据子孙结点的依赖关系(不仅限于元数据内部之间的依赖关系,还可以是内部与外部之间的依赖关系)
	 */
	public List<MMDDependency> genDependencies(final String aNamespace);

	/**
	 * 批量添加元数据依赖关系
	 * 
	 * @param createAMetadata
	 */
	// void batchLoadCreate(AppendMetadata createAMetadata);
	/**
	 * 批量删除元数据依赖关系
	 * 
	 * @param deleteAMetadata
	 */
	// void batchLoadDelete(AppendMetadata deleteAMetadata);
	public Collection<? extends MMDDependency> genDependencies(List<MMMetadata> metadatas);
}
