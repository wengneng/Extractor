package com.pactera.edg.am.metamanager.extractor.bo;

import java.io.Serializable;

/**
 * 作为RMI服务器端与客户端交互使用的对象
 * 
 * @author user
 * 
 */
public class ServerDomain implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3565768980672123L;

	// 返回状态：２：入审核表失败；３：入审核表成功；７：入元数据表成功；８：入元数据表失败　LZ!
	private String status;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public enum Status {
		SUCCESS, // 采集入库成功
		SUCCESS_BUT_ERROR, // 采集入库成功但有错误
		FAILED
		// 采集入库失败
	}

}
