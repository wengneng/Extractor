/*
 * Copyright 2009 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, Beijing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.annotate.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.log4j.Logger;

import com.pactera.edg.am.metamanager.core.ex.BizException;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.annotate.bo.Procedure;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * (简单概要描述此类完成的功能)
 * 
 * @author huanglp
 * @version 1.0 Date: Aug 6, 2009 10:04:23 AM
 * 
 */
public class AnnotateExtractServiceImpl implements IAnnotateExtractService {
	/**
	 * 所有Parser实现类均可用的log.
	 */
	protected static Logger log;

	public AnnotateExtractServiceImpl() {
		log = Logger.getLogger(this.getClass());
	}

	private String annotateFile;

	/**
	 * @return the annotateFile
	 */
	public String getAnnotateFile() {
		return annotateFile;
	}

	/**
	 * @param annotateFile
	 *            the annotateFile to set
	 */
	public void setAnnotateFile(String annotateFile) {
		this.annotateFile = annotateFile;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seecom.pactera.edg.am.metamanager.extractor.adapter.extract.annotate.parse.
	 * IAnnotateExtractService#getAnnotate()
	 */
	public Procedure getAnnotate() {
		Procedure pro = new Procedure();
		pro.setJobName("aaaaa");
		try {
			File file = new File(AdapterExtractorContext.getInstance()
					.getDsAbsolutePath());

			// BufferedReader br = new BufferedReader(new FileReader(new
			// File(annotateFile)));
			BufferedReader br = new BufferedReader(new FileReader(file));
			String lineText = br.readLine();
			boolean beginFlag = false;
			boolean beginMemo = false;
			StringBuffer memoString = new StringBuffer();
			while (lineText != null) {
				lineText = lineText.trim();
				if (lineText.startsWith("/*==")) {
					beginFlag = true;
				}
				if (!beginFlag) {
					lineText = br.readLine();
					continue;
				}
				// 开始读取注释中的内容
				lineText = lineText.replaceAll(" ", "");
				lineText = lineText.replaceAll("：", ":");// 全角：英文:
				lineText = lineText.replaceAll("，", ",");
				String lineFlag = "需求来源:";
				// TODO:1.注释的元模型应该时动态的 2.目标表和源表需要核实是否存在
				if (lineText.startsWith(lineFlag)) {
					String reqSrc = lineText.substring(lineFlag.length(),
							lineText.length());
					if (reqSrc.equals("")) {
						throw new BizException("需求来源信息为空!");
					}
					if (reqSrc.toUpperCase().indexOf(".XLS") == -1
							|| reqSrc.indexOf("_") == -1) {
						throw new BizException("Mapping错误:" + reqSrc);
					}
					pro.setReqSrc(reqSrc);
					log.info("需求来源：" + reqSrc + ":");
				}

				lineFlag = "目标表:";
				if (lineText.startsWith(lineFlag)) {
					String tarTable = lineText.substring(lineFlag.length(),
							lineText.length());
					if (tarTable.equals("")) {
						throw new BizException("目标表信息为空!");
					}
					// 对表 英文名 后的 中文名 进行处理，将其去除 modi by wangrong
					for (int k = 0; k < tarTable.length(); k++) {
						if (tarTable.substring(k, k + 1).matches(
								"[\u4e00-\u9fa5]+")) {
							tarTable = tarTable.replaceAll(tarTable.substring(
									k, k + 1), " ");
						}
					}
					tarTable = tarTable.replaceAll(" _ ", " ");
					tarTable = tarTable.replaceAll(" ", "");
					// TODO:需要支持多个目标表
					log.info("目标表：" + tarTable + ":");
					pro.setTarTable(tarTable);
				}

				lineFlag = "源表:";
				if (lineText.startsWith(lineFlag)) {
					String srcTable = lineText.substring(lineFlag.length(),
							lineText.length());
					if (srcTable.equals("")) {
						throw new BizException("源表信息为空!");
					}
					// 对表 英文名 后的 中文名 进行处理，将其去除 modi by wangrong
					for (int k = 0; k < srcTable.length(); k++) {
						if (srcTable.substring(k, k + 1).matches(
								"[\u4e00-\u9fa5]+")) {
							srcTable = srcTable.replaceAll(srcTable.substring(
									k, k + 1), " ");
						}
					}
					srcTable = srcTable.replaceAll(" _ ", " ");
					srcTable = srcTable.replaceAll(" ", "");
					// TODO:需要支持多个源表
					log.info("源表：" + srcTable + ":");
					pro.setSrcTable(srcTable);
				}

				lineFlag = "备注:";
				if (lineText.startsWith(lineFlag)) {
					if (lineText.indexOf("<mlt>") < 0) {
						String description = lineText.substring(lineFlag
								.length(), lineText.length());
						pro.setDescription(description);
					} else {
						beginMemo = true;
						lineText = br.readLine();
						continue;
					}
				}
				if (lineText.indexOf("</mlt>") != -1) {
					beginMemo = false;
				}
				if (beginMemo) {
					memoString.append(lineText);
					pro.setDescription(memoString.toString());// ?
					log.info("备注：" + memoString.toString() + ":");
				}

				if (lineText.endsWith("==*/")) {
					break;// 注释已经读取完毕
				}
				lineText = br.readLine();
			}
		} catch (Exception e) {
			log.error("", e);
			throw new BizException(e.getMessage());
		}
		return pro;
	}

}
