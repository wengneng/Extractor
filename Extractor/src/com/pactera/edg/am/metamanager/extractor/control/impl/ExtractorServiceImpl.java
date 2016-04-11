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

package com.pactera.edg.am.metamanager.extractor.control.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.core.util.DateUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.control.IExtractorService;
import com.pactera.edg.am.metamanager.extractor.increment.IIncrementAnalysisService;
import com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.AntZip;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;

/**
 * 采集入库控制实现类 注：处理文件形式允许多文件处理，按照文件的最后修改时间升序处理
 * 
 * @author user
 * @version 1.0 Date: Sep 27, 2009
 * 
 */
public class ExtractorServiceImpl implements IExtractorService {

	private static Log log = LogFactory.getLog(ExtractorServiceImpl.class);

	/**
	 * 转换器
	 */
	private IMetadataMappingService mapper;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorService#extract()
	 */
	public void extract() throws Throwable {
		try {
			AdapterExtractorContext.getInstance().setIClassifier(ExtractorContextLoader.getMMRmiServer());

			boolean extractSuccess = false;

			if (AdapterExtractorContext.getInstance().isNeedMultiExtract()) {
				// 一次采集需要有多次采集流程
				extractSuccess = multiExtract();
			}
			else {
				extractSuccess = singleExtract(null);
			}

			if (!extractSuccess) {
				// 数据采集不成功,抛出异常
				throw new Exception("采集数据失败!");
			}
			// 对于增量的数据,才需要删除
			batchDelete();
		}
		finally {

			// ((IMetadataLoaderService)
			// ExtractorContextLoader.getBean(IMetadataLoaderService.SPRING_NAME))
			// .afterOperation();
			AdapterExtractorContext.getInstance().multiExtractClear();
		}
	}

	public static void batchDelete() throws Throwable {
		IIncrementAnalysisService incrementAnalyzer = (IIncrementAnalysisService) ExtractorContextLoader
			.getBean(IIncrementAnalysisService.FULL_INCREMENT_SPRING_NAME);

		Map<Operation, AppendMetadata> aMetadatas = incrementAnalyzer.incrementAnalysis(AdapterExtractorContext
				.getInstance().getAMetadata());

		IMetadataLoaderService loader = (IMetadataLoaderService) ExtractorContextLoader
				.getBean(IMetadataLoaderService.SPRING_NAME);

		if (AdapterExtractorContext.getInstance().isNeedAudit()) {
			// 需要审核,将需要删除的数据写入待审核表
			loader.batchDeleteAudit(aMetadatas);

			loader.batchUpdateOldAuditDatas();
		}
		else {
			// 删除元数据信息,同时将需要删除的数据写入历史表
			loader.batchDeleteMd(aMetadatas);
		}
	}

	/**
	 * 多次采集流程
	 * 
	 * @param incrementAnalyzer
	 * @throws Throwable
	 */
	private boolean multiExtract() throws Throwable {
		File dsDirectorys = new File(AdapterExtractorContext.getInstance().getDsDirectory());
		if (!dsDirectorys.exists()) {
			// 如文件不存在,则抛出异常
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "数据源文件目录不存在:"
					+ AdapterExtractorContext.getInstance().getDsDirectory());
			throw new FileNotFoundException("数据源文件目录不存在:" + AdapterExtractorContext.getInstance().getDsDirectory());
		}

		return multiExtract(dsDirectorys);
	}

	/**
	 * 采集多个文件
	 * 
	 * @param dsDirectorys
	 * @param incrementAnalyzer
	 * @throws Throwable
	 */
	private boolean multiExtract(File dsDirectorys) throws Throwable {
		String s1 = new StringBuilder("开始处理目录：").append(dsDirectorys.getAbsolutePath()).toString();
		log.info(s1);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, s1);

		File[] dsFiles = dsDirectorys.listFiles();
		// 按文件(夹)的文件名排序
		Arrays.sort(dsFiles, new Comparator<File>() {
			public int compare(File f1, File f2) {
				return f1.getName().compareTo(f2.getName());
			}
		});

		// 多次采集是否成功,单次采集是否成功;
		boolean multiExtractSuccess = false, singleExtractSuccess = false;
		int totalCount = dsFiles.length;
		for (int i = 0; i < totalCount; i++) {
			if (dsFiles[i].isDirectory()) {
				multiExtract(dsFiles[i]);// 是一个文件夹
			}
			else if (dsFiles[i].getName().toUpperCase().endsWith(".ZIP")) {// 先处理文件为压缩类型的情况
				// 文件是否为压缩文件
				singleExtractSuccess = this.zipExtract(dsFiles[i]);
				if (!multiExtractSuccess) {
					// 如某一次单次采集成功,则认为多次采集成功
					multiExtractSuccess = singleExtractSuccess;
				}
			}
			else {
				// 一个采集流程出错则所有将出错
				AdapterExtractorContext.getInstance().setDsAbsolutePath(dsFiles[i].getAbsolutePath());
				String s = new StringBuilder("开始处理第").append(i + 1).append("个文件，总共").append(totalCount).append(
						"个。文件路径:").append(dsFiles[i].getAbsolutePath()).toString();
				log.info(s);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, s);
				// 单次采集
				singleExtractSuccess = singleExtract(dsFiles[i].getAbsolutePath());
				if (!multiExtractSuccess) {
					// 如某一次单次采集成功,则认为多次采集成功
					multiExtractSuccess = singleExtractSuccess;
				}
			}
		}
		return multiExtractSuccess;
	}

	/**
	 * 解压缩文件
	 * 
	 * @param zipFile
	 * @param incrementAnalyzer
	 * @return
	 * @throws Throwable
	 */
	private boolean zipExtract(File zipFile) throws Throwable {
		// 先解压压缩文件,解压到当前目录/currenttime的目录
		String extPlace = new StringBuilder(zipFile.getParent()).append(File.separator).append(
				System.currentTimeMillis()).toString();
		File file = new File(extPlace);

		String s2 = new StringBuilder("开始解压缩文件：").append(zipFile.getAbsolutePath()).append("，解压到目录：").append(
				file.getAbsolutePath()).toString();
		log.info(s2);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, s2);

		// 创建新目录
		file.mkdir();
		// 将压缩文件解压至此
		extZipFileList(zipFile, extPlace);

		// 采集多个文件
		boolean zipExtractSuccess = multiExtract(file);

		// 删除解压文件
		try {
			FileUtils.deleteDirectory(file);
		}
		catch (IOException e) {
			log.error("删除解压文件时出现异常!", e);
		}
		return zipExtractSuccess;
	}

	public void extZipFileList(File zipFile, String extPlace) {
		AntZip.unZip(zipFile.getAbsolutePath(), extPlace);
		// try {
		//
		// ZipInputStream in = new ZipInputStream(new FileInputStream(zipFile));
		//
		// ZipEntry entry = null;
		//
		// while ((entry = in.getNextEntry()) != null) {
		//
		// String entryName = entry.getName();
		//
		// if (entry.isDirectory()) {
		// File file = new File(new
		// StringBuilder(extPlace).append(File.separator).append(entryName)
		// .toString());
		// file.mkdirs();
		// log.info("创建文件夹:" + entryName);
		// }
		// else {
		//
		// FileOutputStream os = new FileOutputStream(new
		// StringBuilder(extPlace).append(File.separator)
		// .append(entryName).toString());
		//
		// // Transfer bytes from the ZIP file to the output file
		// byte[] buf = new byte[1024];
		//
		// int len;
		// while ((len = in.read(buf)) > 0) {
		// os.write(buf, 0, len);
		// }
		// os.close();
		// in.closeEntry();
		//
		// }
		// }
		//
		// }
		// catch (IOException e) {
		// e.printStackTrace();
		// }
	}

	/**
	 * 单次采集流程
	 * 
	 * @param incrementAnalyzer
	 * @throws Throwable
	 */
	private boolean singleExtract(String path) {
		Map<Operation, AppendMetadata> aMetadatas = null;
		try {
			IIncrementAnalysisService incrementAnalyzer = (IIncrementAnalysisService) ExtractorContextLoader
					.getBean(IIncrementAnalysisService.INCREMENT_SPRING_NAME);
			String logMsg = "开始采集及转换数据...\n开始时间:"
					+ DateUtil.getFormatTime(System.currentTimeMillis(), DateUtil.DATA_FORMAT);
			log.info(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
			//悬挂结点元数据
			AppendMetadata aMetadata = AdapterExtractorContext.getInstance().getAMetadata().cloneAppendMetadata();
			mapper.metadataMapping(aMetadata);
			//分析、入库操作
			execute(incrementAnalyzer, aMetadata);
			return true;
		}
		catch (Throwable e) {

			if (path != null)
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, new StringBuilder("采集文件:")
						.append(path).append("出现异常!").toString());
			log.error("", e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "" + e.getMessage()); // 防止其为NULL
			return false;
		}
		finally {
			// ((IMetadataLoaderService)
			// ExtractorContextLoader.getBean(IMetadataLoaderService.SPRING_NAME))
			// .afterSingleOperation(aMetadatas);
			cleanScene(aMetadatas);
		}
	}

	public static void execute(IIncrementAnalysisService incrementAnalyzer,
			AppendMetadata aMetadata) throws Throwable {
		Map<Operation, AppendMetadata> aMetadatas = null;
		String logMsg = "";
		logMsg = "采集及转换数据!\n结束时间:"
				+ DateUtil.getFormatTime(System.currentTimeMillis(),
						DateUtil.DATA_FORMAT);
		log.info(logMsg);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);

		log.info("开始作比较分析...");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO,
				"开始作比较分析...");
		aMetadatas = incrementAnalyzer.incrementAnalysis(aMetadata);

		logMsg = "增量分析结束,结束时间:"
				+ DateUtil.getFormatTime(System.currentTimeMillis(),
						DateUtil.DATA_FORMAT);
		log.info(logMsg);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		log.info("开始作批量入库...");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO,
				"开始作批量入库...");

		// 添加全局时间的生成
		AdapterExtractorContext.getInstance().genGlobalTime();
		
		batchCreateOrUpdate(aMetadatas);
		// 删除元数据项目中，明确指定需删除的数据
		deleteAmData(aMetadata.getMetadata().getId(), aMetadata.getDeleteMapDatas());
		logMsg = "批量入库完成，时间:"
				+ DateUtil.getFormatTime(System.currentTimeMillis(),
						DateUtil.DATA_FORMAT) + "。采集尚未结束，请稍候...";
		log.info(logMsg);
		AdapterExtractorContext.getInstance().getIsCover().clear();
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
	}
	
	private static void deleteAmData(String rootId, Map<String, List<Map<String, String>>> deleteMapDatas) {
		if (deleteMapDatas.size() > 0) {
			IMetadataLoaderService loader = (IMetadataLoaderService) ExtractorContextLoader
					.getBean(IMetadataLoaderService.SPRING_NAME);

			if (!AdapterExtractorContext.getInstance().isNeedAudit()) {
				// 删除数据
				loader.deleteData(rootId, deleteMapDatas);
			}
		}

	}

	/**
	 * 清理上下文信息
	 * 
	 * @param metadatas
	 */
	private void cleanScene(Map<Operation, AppendMetadata> metadatas) {
		AdapterExtractorContext.getInstance().singleExtractClear();
	}

	/**
	 * 批量入库
	 * 
	 * @param aMetadatas
	 *            批量入库的对象
	 */
	private static void batchCreateOrUpdate(Map<Operation, AppendMetadata> aMetadatas) throws Throwable {
		IMetadataLoaderService loader = (IMetadataLoaderService) ExtractorContextLoader
				.getBean(IMetadataLoaderService.SPRING_NAME);

		if (AdapterExtractorContext.getInstance().isNeedAudit()) {
			// 需要审核,写入待审核表
			loader.batchCreateOrUpdateAudit(aMetadatas);

			if (!AdapterExtractorContext.getInstance().isFullIncrementCompare()) {
				// 非增量
				loader.batchUpdateOldAuditDatas();
			}
		}
		else {
			// 写入元数据表,包括历史
			loader.batchCreateOrUpdateMd(aMetadatas);
		}

	}

	public void setMapper(IMetadataMappingService mapper) {
		this.mapper = mapper;
	}

}
