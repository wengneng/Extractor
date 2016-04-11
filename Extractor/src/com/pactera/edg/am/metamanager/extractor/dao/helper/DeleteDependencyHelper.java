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
import org.springframework.dao.DataAccessException;

import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 依赖关系DAO层的辅助类,实现PreparedStatementCallback接口,用于实现批量操作依赖关系
 * 
 * @author hqchen
 * @version 1.0 Date: Aug 22, 2009
 */
public class DeleteDependencyHelper extends PreparedStatementCallbackHelper {

	private Log log = LogFactory.getLog(DeleteDependencyHelper.class);
	
	private List<MMDDependency> dependenciesList;

	public DeleteDependencyHelper(int batchSize) {
		super(batchSize);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.jdbc.core.PreparedStatementCallback#doInPreparedStatement(java.sql.PreparedStatement)
	 */
	public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
		String logMsg = "";
		// 从元模型中获取属性该元数据的所有元数据
		Iterator<MMDDependency> dependencies = dependenciesList.iterator();

		long sysTime = AdapterExtractorContext.getInstance().getGlobalTime();
		while (dependencies.hasNext()) {
			MMDDependency dependency = dependencies.next();
			doInPreparedStatement(sysTime, dependency, ps);
		}
		if (super.count % super.batchSize != 0) {
			ps.executeBatch();
			ps.clearBatch();

		}
		logMsg = "删除依赖关系记录数:" + dependenciesList.size();
		log.info(logMsg);
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, logMsg);
		return null;

	}

	private void doInPreparedStatement(long sysTime, MMDDependency dependency, PreparedStatement ps)
			throws SQLException {
		// 元数据父结点ID
		ps.setString(1, dependency.getOwnerMetadata().getId());
		// 元数据ID
		ps.setString(2, dependency.getValueMetadata().getId());

		ps.addBatch();
		ps.clearParameters();
		
		if (++super.count % super.batchSize == 0) {
			ps.executeBatch();
			ps.clearBatch();
		}
	}

	public void setDependenciesList(List<MMDDependency> dependenciesList) {

		this.dependenciesList = dependenciesList;
	}

}
