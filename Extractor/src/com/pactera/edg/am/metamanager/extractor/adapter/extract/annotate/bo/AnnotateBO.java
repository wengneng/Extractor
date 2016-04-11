/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, Beijing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */

/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.annotate.bo;

/**
 * 注释BO
 *
 * @author huanglp
 * @version 1.0  Date: Aug 6, 2009 9:44:37 AM
 *
 */
public abstract class AnnotateBO {
	private String verInfo;//版权信息
	private String jobName;//作业名称
	private String author;//责任人
	private String function;//功能描述
	private String reqSrc;//需求来源
	private String tarTable;//目标表
	private String srcTable;//源表
	private String version;//版本号
	private String loadStrat;//加载策略
	private String description;//备注
	private String modify;//修改历史
	/**
	 * @return the verInfo
	 */
	public String getVerInfo() {
		return verInfo;
	}
	/**
	 * @param verInfo the verInfo to set
	 */
	public void setVerInfo(String verInfo) {
		this.verInfo = verInfo;
	}
	/**
	 * @return the jobName
	 */
	public String getJobName() {
		return jobName;
	}
	/**
	 * @param jobName the jobName to set
	 */
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	/**
	 * @return the author
	 */
	public String getAuthor() {
		return author;
	}
	/**
	 * @param author the author to set
	 */
	public void setAuthor(String author) {
		this.author = author;
	}
	/**
	 * @return the function
	 */
	public String getFunction() {
		return function;
	}
	/**
	 * @param function the function to set
	 */
	public void setFunction(String function) {
		this.function = function;
	}
	/**
	 * @return the reqSrc
	 */
	public String getReqSrc() {
		return reqSrc;
	}
	/**
	 * @param reqSrc the reqSrc to set
	 */
	public void setReqSrc(String reqSrc) {
		this.reqSrc = reqSrc;
	}
	/**
	 * @return the tarTable
	 */
	public String getTarTable() {
		return tarTable;
	}
	/**
	 * @param tarTable the tarTable to set
	 */
	public void setTarTable(String tarTable) {
		this.tarTable = tarTable;
	}
	/**
	 * @return the srcTable
	 */
	public String getSrcTable() {
		return srcTable;
	}
	/**
	 * @param srcTable the srcTable to set
	 */
	public void setSrcTable(String srcTable) {
		this.srcTable = srcTable;
	}
	/**
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}
	/**
	 * @param version the version to set
	 */
	public void setVersion(String version) {
		this.version = version;
	}
	/**
	 * @return the loadStrat
	 */
	public String getLoadStrat() {
		return loadStrat;
	}
	/**
	 * @param loadStrat the loadStrat to set
	 */
	public void setLoadStrat(String loadStrat) {
		this.loadStrat = loadStrat;
	}
	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	/**
	 * @return the modify
	 */
	public String getModify() {
		return modify;
	}
	/**
	 * @param modify the modify to set
	 */
	public void setModify(String modify) {
		this.modify = modify;
	}
	
	
	
}
