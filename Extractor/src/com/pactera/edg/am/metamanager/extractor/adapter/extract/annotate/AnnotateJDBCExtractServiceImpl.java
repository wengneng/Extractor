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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.annotate;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.ICommonExtractService;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;

/**
 * (简单概要描述此类完成的功能)
 * 
 * @author user
 * @version 1.0 Date: Nov 9, 2009
 * 
 */
public class AnnotateJDBCExtractServiceImpl implements ICommonExtractService {

	private JdbcTemplate jdbcTemplate;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.ICommonExtractService#extract()
	 */
	public InputStream extract() {

		// String sql = "SELECT S.name AS PROCEDURE_NAME, S.TEXT FROM DBA_SOURCE
		// S WHERE S.OWNER=? ORDER BY S.name, S.line ASC";
		//
		// jdbcTemplate.query(sql, new Object[] { schName }, new
		// ProcedureRowCallbackHandler(procedureCache));

		return null;
	}

	private class ProcedureRowCallbackHandler implements RowCallbackHandler {

		private Map<String, Procedure> procedureCache;

		public ProcedureRowCallbackHandler(Map<String, Procedure> procedureCache)
		{
			this.procedureCache = procedureCache;
		}

		public void processRow(ResultSet rs) throws SQLException {
			String procName = rs.getString(Procedure.PROCEDURE_NAME);
			if (procedureCache.containsKey(procName)) {
				// 找到存储过程的内容
				procedureCache.get(procName).setText(rs.getString(Procedure.TEXT));
			}

		}

	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

}
