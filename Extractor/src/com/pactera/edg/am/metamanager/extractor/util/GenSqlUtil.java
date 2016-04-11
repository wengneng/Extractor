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

package com.pactera.edg.am.metamanager.extractor.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Element;

import com.pactera.edg.am.metamanager.extractor.ex.SQLFileNotLoadException;

/**
 * 把SQL从配置文件中读取,并写入缓存中 注意:单例实现,只在第一次用到SQL时读取一次
 * 
 * @author hqchen
 * @version 1.0 Date: Jul 9, 2009
 * @version 2.2 2011-02-23 陈富冰 - 通过spring配置文件指定不同数据库对应的SQL执行脚本
 */
public class GenSqlUtil {

	private static Log log = LogFactory.getLog(GenSqlUtil.class);

	/**
	 * 不用数据库的SQL将存储于此，Key为数据库产品名称，Value为SQL的Map集合。
	 */
	private Map<String, Map<String, String>> sqlHolder = new HashMap<String, Map<String, String>>();

	/**
	 * 不同的数据库对应不同文件路径的SQL文件，该参数保存数据库的产品名称和路径键值对。
	 */
	private Map<String, String> locations = new HashMap<String, String>();

	private static final GenSqlUtil instance = new GenSqlUtil();

	/**
	 * 当前MM系统的数据库
	 */
	private String databaseName;

	/**
	 * private creator
	 */
	private GenSqlUtil() {

	}

	/**
	 * factory Getter
	 * 
	 * @return this instance
	 */
	public static GenSqlUtil getInstance() {
		return instance;
	}

	public Map<String, String> getLocations() {
		return locations;
	}

	public void setLocations(Map<String, String> locations) {
		this.locations = locations;
	}

	/**
	 * 返回数据库产品对应SQL脚本文件的路径，如oracle,teradata,sqlserver等
	 * 
	 * @param databaseName
	 *            数据库产品名称
	 * @return 脚本文件的路径，若没有找到则返回null
	 */
	public String getLocation(String databaseName) {
		if (locations.containsKey(databaseName)) { // 名称全匹配的情况
			return locations.get(databaseName);
		}
		for (Iterator<String> it = locations.keySet().iterator(); it.hasNext();) {
			String key = it.next();
			// 正则表达式匹配，如DB2的产品名称可能返回不一样，如：DB2/NT, DB2/LINUX, DB2/SUN
			if (databaseName.matches(key)) { return locations.get(key); }
		}
		return null;
	}

	/**
	 * 根据数据库连接来确定是什么数据库（Oracle、Teradata等），然后确定使用什么语法的SQL文件
	 * 
	 * @return
	 * @throws IOException
	 *             不能获取数据源Bean
	 * @throws SQLException
	 *             不能获取数据库连接，不能确定数据库的类型
	 */
	private static Map<String, String> findDatabaseSQL() {
		Connection conn = null;
		String databaseName = instance.databaseName;
		try {
			if (databaseName == null) {
				DataSource dataSource = ExtractorContextLoader.getDataSource();
				conn = dataSource.getConnection();
				DatabaseMetaData meta = conn.getMetaData();
				instance.databaseName = meta.getDatabaseProductName();
				databaseName = instance.databaseName;
			}
			String location = instance.getLocation(databaseName);
			if (location == null) {
				throw new SQLFileNotLoadException("不能加载数据库【" + databaseName
					+ "】对应的SQL文件，请检查spring/context-service.xml的GenSqlUtil"); }

			if (instance.sqlHolder.get(databaseName) == null) {
				Map<String, String> sqls = readSQL(location);
				instance.sqlHolder.put(databaseName, sqls);
			}
			return instance.sqlHolder.get(databaseName);
		} catch (SQLException e) {
			databaseName = (databaseName == null) ? "Unkown" : databaseName;
			String s = "不能加载数据库[" + databaseName + "]对应的SQL文件";
			log.error(s, e);
			throw new SQLFileNotLoadException(s, e);
		} catch (IOException e) {
			databaseName = (databaseName == null) ? "Unkown" : databaseName;
			String s = "不能加载数据库[" + databaseName + "]对应的SQL文件";
			log.error(s, e);
			throw new SQLFileNotLoadException(s, e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 将资源中的SQL存储于指定内存中
	 * 
	 * @throws IOException
	 *             XML文件不存在
	 */
	private static Map<String, String> readSQL(String location) throws IOException {
		Map<String, String> sqls = new HashMap<String, String>();
		MessageFormat mf = new MessageFormat(Constants.SQL_STORE_PATH);
		String file = mf.format(new Object[] { location });

		InputStream in = null;
		Dom4jReader reader = null;
		try {
			in = GenSqlUtil.class.getClassLoader().getResourceAsStream(file);
			reader = new Dom4jReader();
			reader.initDocument(in);
			List<?> elements = reader.selectNodes(Constants.SQL_FLAG);
			for (int i = 0; i < elements.size(); i++) {
				Element element = (Element) elements.get(i);
				sqls.put(element.attributeValue("id"), element.getTextTrim());
			}
		}
		finally {
			if (reader != null) {
				reader.close();
			}
			if (in != null) {
				in.close();
			}
		}
		return sqls;
	}

	/**
	 * 根据SQLID从内存中获取指定SQL,如内存中不存在相关数据库的SQL,则先装载该数据库类型对应的SQL文件内容。<br>
	 * 由于支持多种数据库的SQL，采集器需要连接何种数据库是由BI.MetaManager的Web端告诉，
	 * 因此每次连接的数据库有可能不一样，举例如：假设MM的Web先连接SQL Server数据库，然后一段时间后某种原因致使该数据库宕机，
	 * 则Web的连接可能切换到Oracle数据库，但MM的采集端并不需要重启，依然可以正常使用，则采集器提供SQL的机制就需要足够灵活。
	 * 
	 * @param sqlId
	 *            定义于XML标签中的ID属性值
	 * @return SQL 对应的SQL，会根据当前是什么数据库来判别应该获取什么样的SQL
	 */
	public static String getSql(String sqlId) {
		return findDatabaseSQL().get(sqlId);
	}
	
	/**
	 * 当数据库连接变更时，清空缓存，以便下次重新根据具体数据库获取对应的SQL
	 */
	public static void restore() {
		instance.databaseName = null;
	}
}
