package com.pactera.edg.am.metamanager.extractor.load.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.load.BaseMetadataLoaderService;
import com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 批量入库控制类
 * 
 * @author user
 * @version 1.0 Date: Jul 7, 2009
 * 
 */
public class MetadataLoaderServiceImpl extends BaseMetadataLoaderService implements IMetadataLoaderService {

	private Log log = LogFactory.getLog(MetadataLoaderServiceImpl.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService#batchCreateOrUpdateAudit(java.util.Map)
	 */
	public synchronized void batchCreateOrUpdateAudit(Map<Operation, AppendMetadata> metadatas) throws Throwable {
		AppendMetadata createAMetadata = metadatas.get(Operation.CREATE);
		// 批量添加元数据
		super.getHarvestMetadataDao().batchLoadCreate(createAMetadata);
		// 批量添加关联关系
		super.getHarvestCompositionDao().batchLoadCreate(createAMetadata);
		// 批量添加依赖关系
		super.getHarvestDependencyDao().batchLoadCreate(createAMetadata);

		AppendMetadata modifyAMetadata = metadatas.get(Operation.MODIFY);

		// 批量修改元数据
		super.getHarvestMetadataDao().batchLoadModify(modifyAMetadata);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService#batchCreateOrUpdateMd(java.util.Map)
	 */
	public synchronized void batchCreateOrUpdateMd(Map<Operation, AppendMetadata> metadatas) throws Throwable {
		AppendMetadata createAMetadata = metadatas.get(Operation.CREATE);
		// 批量添加元数据
		super.getMetadataDao().batchLoadCreate(createAMetadata); // done,check done
		// 记录已添加的元数据至变更信息表
		super.getMetadataAlterDao().alterBatchCreate(createAMetadata); // done,check done
		// 批量添加关联关系
		super.getCompositionDao().batchLoadCreate(createAMetadata); // done,check done
		// 批量添加依赖关系
		super.getDependencyDao().batchLoadCreate(createAMetadata); // done

		AppendMetadata modifyAMetadata = metadatas.get(Operation.MODIFY);

		// 先写历史记录
		super.getHistoryMetadataDao().batchLoadCreate(modifyAMetadata, "M"); // done,check done
		// 批量添加需修改的组合关系--新补充的操作,留意是否会有问题:有问题,操作的方法不对,后续再补充
		// super.getCompositionDao().batchLoadCreate(modifyAMetadata);
		// 记录需修改的元数据至变更信息表
		super.getMetadataAlterDao().alterBatchModify(modifyAMetadata); // done,check done
		// 批量修改元数据
		super.getMetadataDao().batchLoadModify(modifyAMetadata);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService#batchDeleteAudit(java.util.Map)
	 */
	public synchronized void batchDeleteAudit(Map<Operation, AppendMetadata> metadatas) throws Throwable {
		// harvestMetadataDao.----------------
		AppendMetadata deleteAMetadata = metadatas.get(Operation.DELETE);
		// 先将待删除的依赖关系写至审核表
		super.getHarvestDependencyDao().batchLoadDelete(deleteAMetadata);
		// 再将待删除的组合关系写至审核表
		super.getHarvestCompositionDao().batchLoadDelete(deleteAMetadata);
		// 最后将待删除的元数据写至审核表
		super.getHarvestMetadataDao().batchLoadDelete(deleteAMetadata);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService#batchDeleteMd(java.util.Map)
	 */
	public synchronized void batchDeleteMd(Map<Operation, AppendMetadata> metadatas) throws Throwable {
		AppendMetadata deleteAMetadata = metadatas.get(Operation.DELETE);

		// 先将待删除的依赖关系写入至历史依赖关系表中
		super.getHistoryDependencyDao().batchLoadCreate(deleteAMetadata); // done
		// 批量删除依赖关系
		super.getDependencyDao().batchLoadDelete(deleteAMetadata); // done
		// 先将待删除的组合关系写入至历史组合关系表中
		super.getHistoryCompositionDao().batchLoadCreate(deleteAMetadata); // done
		// 批量删除关联关系
		super.getCompositionDao().batchLoadDelete(deleteAMetadata); // done
		// 先将待删除的元数据写入至历史元数据表中
		super.getHistoryMetadataDao().batchLoadCreate(deleteAMetadata, "D");// done
		// 然后将要删除的记录写至变更信息表
		// 因为移植TD的缘故,需删除的记录,如都从库表获取,会与源有结构发生冲突,故此写变更日志的操作暂停
		super.getMetadataAlterDao().alterBatchDelete(deleteAMetadata); // done
		// 最后批量删除元数据
		super.getMetadataDao().batchLoadDelete(deleteAMetadata);
		
		deconstructor();
	}

	/**
	 * 析构，目前只做：已删除的元数据，在数据视图中也相应删除
	 */
	private void deconstructor() {
		super.getDeconstructorDao().deconstrunctor();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService#auditLoadMetadata(java.util.Map)
	 */
	public synchronized void auditLoadMetadata(Map<Operation, AppendMetadata> metadatas) throws Exception {
		AppendMetadata createAMetadata = metadatas.get(Operation.CREATE);
		// 将已确认的待审核元数据写入元数据表
		super.getAuditLoadMetadataDao().auditLoadCreate(createAMetadata);
		// 记录已添加的元数据至变更信息表
		super.getMetadataAlterDao().alterBatchCreate(createAMetadata);
		// 批量添加关联关系
		super.getAuditLoadCompositionDao().auditLoadCreate();
		// 批量添加元数据依赖关系
		super.getAuditLoadDependencyDao().auditLoadCreate();

		AppendMetadata modifyAMetadata = metadatas.get(Operation.MODIFY);
		// 先写历史记录
		super.getHistoryMetadataDao().batchLoadCreate(modifyAMetadata, "M");
		// 记录需修改的元数据至变更信息表
		super.getMetadataAlterDao().alterBatchModify(modifyAMetadata);
		// 批量修改元数据
		super.getAuditLoadMetadataDao().auditLoadModify(modifyAMetadata);

		AppendMetadata deleteAMetadata = metadatas.get(Operation.DELETE);

		// 批量删除待审核表中已确认的需删除的元数据依赖关系(包括写至历史表)
		super.getAuditLoadDependencyDao().auditLoadDelete();
		// 批量删除元数据组合关系(包括写入历史表)
		super.getAuditLoadCompositionDao().auditLoadDelete();
		// 先写历史记录
		super.getHistoryMetadataDao().batchLoadCreate(deleteAMetadata, "D");
		// 然后将要删除的记录写至变更信息表
		super.getMetadataAlterDao().alterBatchDelete(deleteAMetadata);
		// 最后批量删除元数据
		super.getMetadataDao().batchLoadDelete(deleteAMetadata);

		// 审核入库完成,开始将审核记录删除
		log.info("审核入库完成,开始将审核记录删除...");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "审核入库完成,开始将审核记录删除...");

		super.getAuditLoadDependencyDao().delete();
		super.getAuditLoadCompositionDao().delete();
		super.getAuditLoadMetadataDao().delete();

		log.info("删除审核记录完成!");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "删除审核记录完成!");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService#batchUpdateOldAuditDatas()
	 */
	public void batchUpdateOldAuditDatas() {

		log.info("更新旧有待审核数据的状态...");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "更新旧有待审核数据的状态...");

		super.getHarvestDependencyDao().updateOldDatas();
		super.getHarvestCompositionDao().updateOldDatas();
		super.getHarvestMetadataDao().updateOldDatas();

		log.info("更新旧有待审核数据的状态完成!");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "更新旧有待审核数据的状态完成!");

	}

	public void deleteData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas) {
		super.getDeconstructorDao().deleteData(rootId, deleteMapDatas);
	}
	
	public void deleteMappingData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas) {
		super.getDeconstructorDao().deleteMappingData(rootId, deleteMapDatas);
	}

}
