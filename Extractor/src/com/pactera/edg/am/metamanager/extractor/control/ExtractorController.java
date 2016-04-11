package com.pactera.edg.am.metamanager.extractor.control;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.app.bo.TaskInstance;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.ServerDomain;
import com.pactera.edg.am.metamanager.extractor.control.impl.ExtractorServiceImpl;
import com.pactera.edg.am.metamanager.extractor.ex.SpringContextLoadException;
import com.pactera.edg.am.metamanager.extractor.ex.TaskInstanceNotFoundException;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;
import com.pactera.edg.am.metamanager.extractor.util.MapperFactory;

/**
 * 采集器的总控程序，对数据源的采集，映射，增量分析，入库。其中增量分析，入库需作线程同步
 * 
 * @author user
 * @version 1.0 Date: Jul 6, 2009
 * 
 */
public final class ExtractorController {

	private Log log = LogFactory.getLog(ExtractorController.class);

	public ExtractorController()
	{

	}

	/**
	 * 採集數據，增量分析，入庫
	 * 
	 * @param taskInstanceId
	 * @return
	 */
	public ServerDomain adapterControl(String taskInstanceId) {
		ServerDomain sd = new ServerDomain();
		long startTime = System.currentTimeMillis();

		try {
			genTaskInstanceConf(taskInstanceId);

			// 1.根据taskId获取适配器类型
			// 1.初始化,根据条件获取转换器,由转换器工厂产生转换器
			IMetadataMappingService mapper = MapperFactory.createMapper(AdapterExtractorContext.getInstance()
					.getMapperConfs(), AdapterExtractorContext.getInstance().getParameters());

			beforeExtract();

			ExtractorServiceImpl extractorImpl = new ExtractorServiceImpl();
			extractorImpl.setMapper(mapper);

			extractorImpl.extract();

			if (AdapterExtractorContext.getInstance().isExtractorLoggerUseable()) {
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "数据采集入库顺利完成!");
			}
			log.info("数据采集入库顺利完成，本次采集结束!");
			
			if (AdapterExtractorContext.getInstance().isNeedAudit()) {
				AdapterExtractorContext.getInstance().setReturnStatus(TaskInstance.STATE_TO_APPROVE);
			}
			else {
				if(AdapterExtractorContext.getInstance().getReturnStatus() == null)
					AdapterExtractorContext.getInstance().setReturnStatus(TaskInstance.STATE_IMPORTED);
			}
		}
		catch (Throwable t) {
			log.error("采集数据出现异常:" + t.getMessage(), t);
			if (AdapterExtractorContext.getInstance().isExtractorLoggerUseable()) {
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "采集数据出现异常:" + t.getLocalizedMessage());
			}
			if (AdapterExtractorContext.getInstance().isNeedAudit()) {
				AdapterExtractorContext.getInstance().setReturnStatus(TaskInstance.STATE_HARVEST_FAILED);
			}
			else {
				AdapterExtractorContext.getInstance().setReturnStatus(TaskInstance.STATE_IMPORT_FAILED);
			}
		}
		finally {

			// 5.清理现场
			String logMsg = "本次采集总花费时间: " + (System.currentTimeMillis() - startTime)/1000F + "s";
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
			// 最后手动触发写日志
			AdapterExtractorContext.getInstance().getEObservable().recordLogManual();

			// 最后清理部分数据
			AdapterExtractorContext.getInstance().finallyClear();
		}
		sd.setStatus(AdapterExtractorContext.getInstance().getReturnStatus());
		AdapterExtractorContext.getInstance().setServerDomain(sd);

		return sd;
	}

	private void beforeExtract() {
		((IExtractorConfLoader) ExtractorContextLoader.getBean(IExtractorConfLoader.SPRING_NAME)).getTargetDBInfo();
	}

	/**
	 * 获取任务实例的相关配置信息
	 * 
	 * @param taskInstanceId
	 *            任务实例ID
	 * @throws SpringContextLoadException
	 *             如SPRING配置初始化失败,则抛出该异常
	 * @throws TaskInstanceNotFoundException
	 *             如不存在该任务实例,则抛出该异常
	 */
	private void genTaskInstanceConf(String taskInstanceId) throws SpringContextLoadException,
			TaskInstanceNotFoundException, SQLException {
		IExtractorConfLoader confLoader = (IExtractorConfLoader) ExtractorContextLoader
				.getBean(IExtractorConfLoader.SPRING_NAME);
		try {
			confLoader.genTaskInstanceConf(taskInstanceId);
		}
		catch (SQLException e) {
			log.error("连接元数据库并获取任务相关的配置信息时,发生异常,请确认数据库连接参数是否正确!", e);
			AdapterExtractorContext
					.addExtractorLog(ExtractorLogLevel.ERROR, "连接元数据库并获取任务相关的配置信息时,发生异常,请确认数据库连接参数是否正确!");
			throw e;
		}
	}

}
