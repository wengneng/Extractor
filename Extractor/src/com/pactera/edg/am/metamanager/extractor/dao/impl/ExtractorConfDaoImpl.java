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

package com.pactera.edg.am.metamanager.extractor.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import com.pactera.edg.am.metamanager.app.bo.THarvestDatasource;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorTask;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.DaoBaseServiceImpl;
import com.pactera.edg.am.metamanager.extractor.dao.IExtractorConfDao;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao;
import com.pactera.edg.am.metamanager.extractor.ex.TaskInstanceNotFoundException;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 采集相关配置DAO操作类
 * 
 * @author hqchen
 * @version 1.0 Date: Aug 6, 2009
 */
public class ExtractorConfDaoImpl extends DaoBaseServiceImpl implements
		IExtractorConfDao {

	private Log log = LogFactory.getLog(ExtractorConfDaoImpl.class);

	private IMetadataDao metadataDao;

	public void setMetadataDao(IMetadataDao metadataDao) {
		this.metadataDao = metadataDao;
	}

	/**
	 * 根据任务ID获取任务相关信息
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IExtractorConfDao#queryTask(String)
	 */
	public ExtractorTask queryTask(final String taskInstanceId) {
		String sql = super.getSql("QUERY_EXTRACTOR_TASK");
		try {
			return (ExtractorTask) super.getJdbcTemplate().queryForObject(sql,
					new Object[] { taskInstanceId },
					new ExtractorTaskMapper(taskInstanceId));
		} catch (EmptyResultDataAccessException e) {
			// 返回记录为空
			String logMsg = new StringBuilder("存储库中不存在ID为:")
					.append(taskInstanceId).append("的采集任务信息").toString();
			log.error(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					logMsg);
			if (log.isDebugEnabled()) {
				logMsg = new StringBuilder("存储库中不存在ID为:")
						.append(taskInstanceId).append("的采集任务信息").toString();
				log.error(logMsg, e);
				AdapterExtractorContext.addExtractorLog(
						ExtractorLogLevel.ERROR, logMsg);
			}
		}
		return new ExtractorTask();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IExtractorConfDao#queryDatasource(String)
	 */
	public THarvestDatasource queryDatasource(final String taskInstanceId) {
		ExtractorTask task = this.queryTask(taskInstanceId);
		String datasourceId = task.getDatasourceId();
		if (datasourceId == null || "".equals(datasourceId)) {// 不存在数据源
			return new THarvestDatasource();
		}
		return queryDatasourceById(datasourceId);
	}

	private THarvestDatasource queryDatasourceById(final String datasourceId) {
		String sql = super.getSql("QUERY_HARVEST_DATASOURCE");
		try {
			return (THarvestDatasource) super.getJdbcTemplate().queryForObject(
					sql, new Object[] { datasourceId },
					new DatasourceMapper(datasourceId));
		} catch (EmptyResultDataAccessException e) {
			// 返回记录为空
			String logMsg = new StringBuilder("存储库中不存在ID为:")
					.append(datasourceId).append("的数据源信息").toString();
			log.error(logMsg);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					logMsg);
			if (log.isDebugEnabled()) {
				logMsg = new StringBuilder("存储库中不存在ID为:").append(datasourceId)
						.append("的数据源信息").toString();
				log.error(logMsg, e);
			}
		}
		return new THarvestDatasource();
	}

	public void queryUser(final String taskInstanceId) {
		String userId = null;
		try {
			userId = (String) super.getJdbcTemplate().queryForObject(
					super.getSql("QUERY_USER"),
					new Object[] { taskInstanceId }, String.class);
		} catch (EmptyResultDataAccessException e) {
			log.error("任务实例的创建者为空！");
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"任务实例的创建者为空！");
		}
		try {
			if (userId == null) {
				List<?> userIds = super.getJdbcTemplate().queryForList(
						super.getSql("QUERY_USER_ON_APPROVE"),
						new Object[] { taskInstanceId }, String.class);
				if (userIds != null && userIds.size() > 0) {
					userId = (String) userIds.get(0);
				}
			}
		} catch (EmptyResultDataAccessException e) {
			log.error("任务实例的创建者为空！");
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"任务实例的创建者为空！");
		}
		if (userId == null) {
			userId = "unknown";
		}
		AdapterExtractorContext.getInstance().setUserId(userId);
		log.info("taskInstanceId:" + taskInstanceId + ",userId:" + userId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.dao.IExtractorConfDao#genTaskInstanceConf(String)
	 */
	public void genTaskInstanceConf(String taskInstanceId)
			throws TaskInstanceNotFoundException, SQLException {
		String sql = super.getSql("QUERY_TASK_INSTANCE");
		ExtractorTaskInstance instance = null;
		try {
			instance = (ExtractorTaskInstance) super.getJdbcTemplate()
					.queryForObject(sql, new Object[] { taskInstanceId },
							new ExtractorTaskInstanceMapper(taskInstanceId));
			if (instance.getImportStrat() == null) {
				throw new TaskInstanceNotFoundException(
						"库表中不存在任务实例,请核查数据!数据ID为" + taskInstanceId);
			}
		} catch (DataIntegrityViolationException e) {
			throw new TaskInstanceNotFoundException("库表中不存在任务实例,请核查数据!数据ID为"
					+ taskInstanceId);
		} catch (EmptyResultDataAccessException e) {
			throw new TaskInstanceNotFoundException("库表中不存在任务实例,请核查数据!数据ID为"
					+ taskInstanceId);
		}

		AdapterExtractorContext.getInstance().setUserId(instance.getUserId());
		AdapterExtractorContext.getInstance().setNeedAudit(
				instance.getNeedAudit());

		if ("0".equals(instance.getImportStrat())) {
			// 全量比较
			AdapterExtractorContext.getInstance().setFullIncrementCompare(true);
		}

		THarvestDatasource datadource = queryDatasourceById(instance
				.getDatasourceId());
		AdapterExtractorContext.getInstance().setDatasourceId(
				instance.getDatasourceId());

		// 元数据悬挂结点
		AppendMetadata aMetadata = genAMetadata(datadource.getDataPath());
		AdapterExtractorContext.getInstance().setAMetadata(aMetadata);

		genTaskParameters(taskInstanceId);

	}

	private AppendMetadata genAMetadata(String metadataId) {
		return metadataDao.queryAMetadata(metadataId);
	}

	private void genTaskParameters(String taskInstanceId) {
		String sql = super.getSql("QUERY_TASK_PARAMETERS");
		final Properties parameters = new Properties();
		final Set<String> mapperConfs = new HashSet<String>(2);
		super.getJdbcTemplate().query(sql, new Object[] { taskInstanceId },
				new RowCallbackHandler() {

					public void processRow(ResultSet rs) throws SQLException {
						String key = rs.getString("PARAM_NAME");
						if ("mapper-spring".equals(key)) {
							if (null != rs.getString("PARAM_VALUE")
									&& !rs.getString("PARAM_VALUE").equals("")) {
								mapperConfs.add(new StringBuilder(
										Constants.SPRING_DIRECTORY).append(
										rs.getString("PARAM_VALUE")).toString());
								//（华润），判断是否为文件上传方式
								AdapterExtractorContext.getInstance().setFileUpload(false);
							}
						} else if ("adapter-spring".equals(key)) {
							if (null != rs.getString("PARAM_VALUE")
									&& !rs.getString("PARAM_VALUE").equals("")) {
								mapperConfs.add(new StringBuilder(
										Constants.SPRING_DIRECTORY).append(
										rs.getString("PARAM_VALUE")).toString());
								//（华润），判断是否为文件上传方式
								AdapterExtractorContext.getInstance().setFileUpload(false);
							}
						} else if ("filePath".equals(key)) {
							// 有filePath,表示为手动采集,指定的为一个目录,将可能有多次采集流程
							AdapterExtractorContext.getInstance()
									.setNeedMultiExtract(true);
							AdapterExtractorContext
									.getInstance()
									.setDsDirectory(rs.getString("PARAM_VALUE"));

							parameters.setProperty(key,
									rs.getString("PARAM_VALUE"));
							//（华润），判断是否为文件上传方式
							AdapterExtractorContext.getInstance().setFileUpload(true);
						} else if ("dsMdId".equals(key)) {
							// Excel适配器专用的参数:放置悬挂结点ID
							// 元数据悬挂结点
							AppendMetadata aMetadata = genAMetadata(rs
									.getString("PARAM_VALUE"));
							AdapterExtractorContext.getInstance().setAMetadata(
									aMetadata);
						} else {
							String value = rs.getString("PARAM_VALUE");
							if (value == null) {
								value = "";
							}
							parameters.setProperty(key, value);
						}
					}

				});

		String[] confs = new String[mapperConfs.size()];
		AdapterExtractorContext.getInstance().setMapperConfs(
				mapperConfs.toArray(confs));
		boolean isMapperCGB = false;
		boolean isAdapterCGB = false;
		for (String str : mapperConfs) {
			if (!"".equals(str) && str.endsWith("adapter-db-cgb.xml")) {
				isAdapterCGB = true;
			} else if (!"".equals(str) && str.endsWith("mapper-db-cgb.xml")) {
				isMapperCGB = true;
			}
		}
		if (isAdapterCGB && isMapperCGB) {
			AdapterExtractorContext.getInstance().setIsDBExtract(true);
		}else{
			AdapterExtractorContext.getInstance().setIsDBExtract(false);
		}
		AdapterExtractorContext.getInstance().setParameters(parameters);

	}

	private class DatasourceMapper implements RowMapper {

		private String datasourceId;

		public DatasourceMapper(String datasourceId) {
			this.datasourceId = datasourceId;
		}

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			THarvestDatasource datasource = new THarvestDatasource();
			datasource.setDataPath(rs.getString("DATA_PATH"));
			datasource.setDatasourceId(datasourceId);
			datasource.setDatasourceName(rs.getString("DATASOURCE_NAME"));
			datasource.setDatasourceTypeCode(rs
					.getString("DATASOURCE_TYPE_CODE"));
			datasource.setDescription(rs.getString("DESCRIPTION"));
			datasource.setModeId(rs.getString("MODE_ID"));
			return datasource;
		}

	}

	private class ExtractorTaskInstance {
		private String taskInstanceId;

		private String taskId;

		private String importStrat;

		// 是否需审核
		private boolean needAudit;

		private String datasourceId;

		private String userId;

		public String getTaskInstanceId() {
			return taskInstanceId;
		}

		public void setTaskInstanceId(String taskInstanceId) {
			this.taskInstanceId = taskInstanceId;
		}

		public String getTaskId() {
			return taskId;
		}

		public void setTaskId(String taskId) {
			this.taskId = taskId;
		}

		public String getImportStrat() {
			return importStrat;
		}

		public void setImportStrat(String importStrat) {
			this.importStrat = importStrat;
		}

		public String getDatasourceId() {
			return datasourceId;
		}

		public void setDatasourceId(String datasourceId) {
			this.datasourceId = datasourceId;
		}

		public boolean getNeedAudit() {
			return needAudit;
		}

		public void setNeedAudit(boolean needAudit) {
			this.needAudit = needAudit;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}

		public String getUserId() {
			return userId;
		}

	}

	/**
	 * 采集任务实例配置的ORM组装类
	 * 
	 * @author user
	 * @version 1.0 Date: Aug 6, 2009
	 * 
	 */
	private class ExtractorTaskInstanceMapper implements RowMapper {

		private String taskInstanceId;

		public ExtractorTaskInstanceMapper(String taskInstanceId) {
			this.taskInstanceId = taskInstanceId;
		}

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			ExtractorTaskInstance instance = new ExtractorTaskInstance();
			instance.setTaskInstanceId(taskInstanceId);

			instance.setImportStrat(rs.getString("IMPORT_STRAT"));
			instance.setDatasourceId(rs.getString("DATASOURCE_ID"));
			// 添加获取userId
			instance.setUserId(rs.getString("USERID"));
			if ("1".equals(rs.getString("IMPORT_AUDIT"))) {
				// 需审核
				instance.setNeedAudit(true);
			}

			return instance;
		}

	}

	/**
	 * 采集任务配置的ORM组装类
	 * 
	 * @author user
	 * @version 1.0 Date: Aug 6, 2009
	 * 
	 */
	private class ExtractorTaskMapper implements RowMapper {
		private String taskInstanceId;

		public ExtractorTaskMapper(String taskInstanceId) {
			this.taskInstanceId = taskInstanceId;
		}

		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
			ExtractorTask et = new ExtractorTask(taskInstanceId);
			et.setName(rs.getString("TASK_NAME"));
			et.setDescription(rs.getString("DESCRIPTION"));
			et.setDatasourceId(rs.getString("DATASOURCE_ID"));
			return et;
		}
	}

}
