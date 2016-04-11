package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.impl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.IJiCaiDBExtractService;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 数据库JDBC采集服务的FactoryBean，根据不同的数据库提供不同的采集服务实现
 * 
 * @author fbchen
 * @version 1.0 2010-07-07
 */
public class JiCaiDBExtractServiceFactoryBean implements FactoryBean,
		BeanClassLoaderAware, InitializingBean {

	private static final String OTHER_TYPE = "Other";

	private Properties pro = AdapterExtractorContext.getInstance()
			.getParameters();
	/**
	 * 连接驱动，可以考虑根据连接串的关键字来判断使用什么采集器
	 */
	private String driverClassName;

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	/**
	 * 数据源，可以考虑根据数据源的连接来判断使用什么采集器
	 */
	private DataSource dataSource;

	/**
	 * 所有的采集器，请参考文件extractor/spring/context-adapter-db-1.xml
	 */
	private Map<String, IJiCaiDBExtractService> dbExtractors = new HashMap<String, IJiCaiDBExtractService>();

	public Map<String, IJiCaiDBExtractService> getDbExtractors() {
		return dbExtractors;
	}

	public void setDbExtractors(Map<String, IJiCaiDBExtractService> dbExtractors) {
		this.dbExtractors = dbExtractors;
	}

	/**
	 * Set the JDBC DataSource to obtain connections from.
	 */
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * Return the DataSource used by this template.
	 */
	public DataSource getDataSource() {
		return this.dataSource;
	}

	/**
	 * 返回采集器。根据数据库产品名称来查找是否有专用的DB采集适配器，有则返回，没有则返回通用的适配器。
	 */
	public Object getObject() throws Exception {
		String name = pro == null ? getDatabaseProductName() : pro.getProperty("dbType");
		if (name == null) {// 名称全匹配的情况
			return dbExtractors.get(OTHER_TYPE);
		}
		for (Iterator<String> it = dbExtractors.keySet().iterator(); it
				.hasNext();) {
			String key = it.next();
			// 正则表达式匹配，如DB2的产品名称可能返回不一样，如：DB2/NT, DB2/LINUX, DB2/SUN
			if (name.matches(key)) {
				return dbExtractors.get(key);
			}
		}
		// 没有开发专用的DB采集适配器，则返回默认的
		return dbExtractors.get(OTHER_TYPE);
	}

	/**
	 * 获取DB的数据库类型，首选方式为根据Connection中的元数据信息获取知道数据库的产品名称。
	 * 这里先根据Driver的名称判断，若无法判断到则根据Connection连接的DatabaseProductName属性来判断。
	 * @return 数据库产品名称
	 * @throws SQLException 连接异常
	 */
	protected String getDatabaseProductName() throws java.sql.SQLException {
		String type = null;
		for (Iterator<String> dbTypes = dbExtractors.keySet().iterator(); dbTypes.hasNext();) {
			String dbType = dbTypes.next();
			if (driverClassName.toUpperCase().indexOf(dbType) >= 0) {
				type = dbType;
				break;
			}
		}
		
		if (type == null) {
			java.sql.Connection conn = null;
			try {
				conn = dataSource.getConnection();
				java.sql.DatabaseMetaData meta = conn.getMetaData();
				type = meta.getDatabaseProductName();
			} finally {
				if (conn != null) {
					conn.close();
				}
			}
		}
		return type;
	}

	
	/**
	 * 返回采集器类型
	 */
	public Class<?> getObjectType() {
		return IJiCaiDBExtractService.class;
	}

	public boolean isSingleton() {
		return true;
	}

	public void setBeanClassLoader(ClassLoader classLoader) {

	}

	public void afterPropertiesSet() throws Exception {

	}

}
