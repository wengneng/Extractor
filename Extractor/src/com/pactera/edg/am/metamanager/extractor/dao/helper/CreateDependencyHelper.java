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

package com.pactera.edg.am.metamanager.extractor.dao.helper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 添加元数据依赖关系操作PreparedStatementCallback辅助类
 * 
 * @author hqchen
 * @version 1.0 Date: Aug 21, 2009
 */
public class CreateDependencyHelper extends PreparedStatementCallbackHelper {

	private Log log = LogFactory.getLog(CreateDependencyHelper.class);

	protected List<MMDDependency> dependenciesList;

	public CreateDependencyHelper(int batchSize)
	{
		super(batchSize);
	}

	public void setDependenciesList(List<MMDDependency> dependenciesList) {
		this.dependenciesList = dependenciesList;
	}

	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException {
		String logMsg = "";
		// 从元模型中获取属性该元数据的所有元数据

		long sysTime = AdapterExtractorContext.getInstance().getGlobalTime();
		for (Iterator<MMDDependency> dependencies = dependenciesList.iterator(); dependencies.hasNext();) {
			MMDDependency dependency = dependencies.next();
			myDoInPreparedStatement(sysTime, dependency, ps);
		}

		if (super.count % super.batchSize != 0) {
			ps.executeBatch();
			ps.clearBatch();

		}
		logMsg = "添加依赖关系记录数:" + dependenciesList.size();
		log.info(logMsg);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		return null;

	}

	private void myDoInPreparedStatement(long sysTime, MMDDependency dependency, PreparedStatement ps)
			throws SQLException {

		String ownerModelId = dependency.getOwnerMetadata().getClassifierId();
		String valueModelId = dependency.getValueMetadata().getClassifierId();

		try {
			// 元数据父结点ID
			ps.setString(1, dependency.getOwnerMetadata().getId());
			// 元数据父结点模型ID
			ps.setString(2, ownerModelId);
			// 元数据ID
			ps.setString(3, dependency.getValueMetadata().getId());
			// 元数据模型ID
			ps.setString(4, valueModelId);
			// 依赖关系名称
			ps.setString(5, dependency.getCode());
			// 系统时间
			ps.setLong(6, sysTime);
			ps.setString(7, "1");
			ps.setString(8, "1");

			setPs(ps, 8);

			ps.addBatch();
			// 每次都清空参数
			ps.clearParameters();
			if (++super.count % super.batchSize == 0) {
				ps.executeBatch();
				ps.clearBatch();
			}
		}
		catch (SQLException e) {
			String logMsg = new StringBuilder().append("数据异常,请核实数据:依赖端元数据ID:").append(
					dependency.getOwnerMetadata().getId()).append(",依赖端元模型:").append(ownerModelId)
					.append(",被依赖端元数据ID:").append(dependency.getValueMetadata().getId()).append(",被依赖端元模型:").append(
							valueModelId).append(", 依赖关系名称:").append(dependency.getCode()).toString();
			log.error(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, logMsg);
			throw e;
		}
	}

	protected void setPs(PreparedStatement ps, int index) throws SQLException {

	}

}
