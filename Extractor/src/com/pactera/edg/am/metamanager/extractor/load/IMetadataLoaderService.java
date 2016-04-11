package com.pactera.edg.am.metamanager.extractor.load;

import java.util.List;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;

/**
 * 批量入库接口
 * 
 * @author user
 * @version 1.0 Date: Jul 7, 2009
 * 
 */
public interface IMetadataLoaderService {

	final String SPRING_NAME = "iMetadataLoaderService";

	/**
	 * 批量添加,修改增量数据,至待审核系列表中(包括待审核元数据表,待审核组合关系表,待审核依赖关系表)
	 * 
	 * @param metadatas
	 *            增量数据
	 * @throws Throwable
	 */
	void batchCreateOrUpdateAudit(Map<Operation, AppendMetadata> metadatas) throws Throwable;

	/**
	 * 批量添加,修改增量数据,至元数据系列表中(包括元数据表,组合关系表,依赖关系表);同时包括对修改元数据写至历史元数据表;
	 * 添加,修改的增量数据写至变更信息表(只有元数据有变更信息表)
	 * 操作顺序:添加元数据至元数据表;添加的元数据写至变更信息表;添加组合关系至元数据组合关系表;添加依赖关系至依赖关系表;
	 * 将需修改的数据添加至历史元数据表;将需修改的数据添加至变更信息表;更新元数据表 缺少修改的元数据的组合关系写入至历史组合关系表??
	 * 
	 * @param metadatas
	 *            增量数据
	 * @throws Throwable
	 */
	void batchCreateOrUpdateMd(Map<Operation, AppendMetadata> metadatas) throws Throwable;

	/**
	 * 将需删除的增量数据,批量写至待审核元数据系列表中(包括待审核元数据表,待审核组合关系表,待审核依赖关系表)
	 * 
	 * @param metadatas
	 *            增量数据
	 * @throws Throwable
	 */
	void batchDeleteAudit(Map<Operation, AppendMetadata> metadatas) throws Throwable;

	/**
	 * 根据需删除的增量数据,批量删除元数据系列表中的记录(包括元数据表,组合关系表,依赖关系表);同时包括对删除元数据系列信息,
	 * 写至历史元数据系列表(历史元数据表,历史组合关系表,历史依赖关系表);需删除的增量数据写至变更信息表(只有元数据有变更信息表)
	 * 操作顺序:将待删除的依赖关系写入至历史依赖关系表,然后删除依赖关系;将待删除的组合关系写入至历史组合关系表,然后删除组合关系;
	 * 将待删除的元数据写入至历史元数据表;将待删除的元数据写入至变更信息表;最后删除元数据
	 * 
	 * @param metadatas
	 *            增量数据
	 * @throws Throwable
	 */
	void batchDeleteMd(Map<Operation, AppendMetadata> metadatas) throws Throwable;

	/**
	 * 审核入库元数据系列信息:将待审核表中已确认的元数据系列信息,同步操作至元数据系列表(包括添加,修改,删除相应的元数据记录,组合关系记录,依赖关系记录)
	 * 同时将需要修改,删除的元数据记录,组合关系记录,依赖关系记录写至历史元数据系列表;将需添加,修改,删除的元数据信息,写至变更信息表
	 * 
	 * @param metadatas
	 *            已确认的待审核数据
	 * @throws Throwable
	 */
	void auditLoadMetadata(Map<Operation, AppendMetadata> metadatas) throws Exception;

	/**
	 * 写待审核表完毕,保证数据库中只有一份数据是状态为待审核的,其它数据如相同,则修改状态为撤销
	 */
	void batchUpdateOldAuditDatas();

	// 增量模式下的删除数据
	void deleteData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas);
	
	// 增量模式下的删除映射数据
	void deleteMappingData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas);

	// /**
	// * 处理正常操作之后的回滚工作，如写历史表失败，则要求历史数据回滚
	// */
	// void afterOperation();
	//
	// /**
	// * 处理单次入库失败之后的数据回滚,包括:
	// * 1.回滚历史数据,包括历史依赖关系,历史组合关系,历史元数据:如下情形会触发该动作:
	// * 将需删除的组合关系写入至历史组合关系表失败;将需修改的元数据写入至历史元数据表失败;
	// * 将需删除的依赖关系写入至历史依赖关系表失败;缺少需修改的元数据的组合关系写入至历史组合关系表失败??
	// * 或删除组合关系失败;或删除依赖关系失败;删除元数据失败;修改元数据失败;
	// *
	// * 2.回滚变更信息:如下情形会触发该动作:将需添加的元数据写入至变更信息表失败
	// *
	// * 3.回滚添加的元数据表:如下情形会触发该动作:添加元数据失败
	// * @param aMetadatas
	// */
	// void afterSingleOperation(Map<Operation, AppendMetadata> aMetadatas);

}
