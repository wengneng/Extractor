package com.pactera.edg.am.metamanager.extractor.control.impl;

import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Element;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.NestedRuntimeException;

import com.pactera.edg.am.metamanager.core.util.FormatUtil;
import com.pactera.edg.am.metamanager.core.util.XMLTree;
import com.pactera.edg.am.metamanager.extractor.adapter.support.ITestConnection;
import com.pactera.edg.am.metamanager.extractor.adapter.support.TestConnectionException;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.ServerDomain;
import com.pactera.edg.am.metamanager.extractor.control.ExtractRunnable;
import com.pactera.edg.am.metamanager.extractor.control.IAuditService;
import com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote;
import com.pactera.edg.am.metamanager.extractor.control.RMIServerStarter;
import com.pactera.edg.am.metamanager.extractor.ex.HarvestAdapterException;
import com.pactera.edg.am.metamanager.extractor.ex.InadequateResourcesException;
import com.pactera.edg.am.metamanager.extractor.util.AdapterContextLoader;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;
import com.pactera.edg.am.metamanager.extractor.util.GenSqlUtil;

/**
 * 采集元数据接口的实现
 * 
 * @author hqchen
 * @version 1.0 2010-03-30
 * @author fbchen 修改读取路径的方式：采用org.springframework.core.io.DefaultResourceLoader
 */
public class ExtractorRemoteImpl implements IExtractorRemote {
	private static Log log = LogFactory.getLog(ExtractorRemoteImpl.class);

	private Thread extractThread;

	public ExtractorRemoteImpl() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote#isRunning()
	 */
	public boolean isRunning() {
		return RMIServerStarter.isStarted();
	}

	/*
	 * (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote#reportState()
	 */
	public String reportState() {
		XMLTree xml = null;
		try {
			xml = new XMLTree();
			Element root = xml.getRootElement();
			AdapterExtractorContext context = AdapterExtractorContext.getInstance();
			xml.addElementText(root, "running", String.valueOf(isRunning()));
			xml.addElementText(root, "harvesting", String.valueOf(extractThread != null));
			xml.addElementText(root, "taskInstanceId", context.getTaskInstanceId());
			xml.addElementText(root, "datasourceId", context.getDatasourceId());
			xml.addElementText(root, "userId", context.getUserId());
		} catch (Exception e) {
			log.error("生成RMI-XML报告异常", e);
			e.printStackTrace();
		}
		return xml != null ? xml.toXmlString() : null;
	}

	/**
	 * 中断采集的方法
	 * 不安全,严重不建议用!!例如打开了数据库连接并正进行修改数据操作,此时执行中断的话,则会造成:1.数据库连接没有释放;2.数据库数据的完整性无法保证
	 * 是否考虑在几个关键点设置是否关闭的标记符,据此来退出?就采集程序来说,貌似还比较复杂....-_-
	 * 
	 * @return
	 */
	@Deprecated
	public boolean stop() {
		if (extractThread != null) {
			// 不安全的线程关闭方法
			extractThread.stop();
			// 参数复位
			AdapterExtractorContext.getInstance().finallyClear();
			return true;
		}
		// 采集已经完成
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote#adapterMetadata(String)
	 */
	public ServerDomain adapterMetadata(String taskInstanceId) throws HarvestAdapterException {
		if (extractThread != null) {
			// 只允许一个采集实例运行,如有多个进来,则不采集
			throw new InadequateResourcesException("已有采集任务在运行中,其任务实例ID:"
					+ AdapterExtractorContext.getInstance().getTaskInstanceId());
		}

		// 缓存任务实例ID
		AdapterExtractorContext.getInstance().setTaskInstanceId(taskInstanceId);
		log.info("-------------------------------------\n采集程序开始运作...");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO,
				"-------------------------------------\n采集程序开始运作...");
		AdapterExtractorContext.getInstance().getEObservable().recordLogManual();

		try {
			extractThread = new Thread(new ExtractRunnable(taskInstanceId));
			extractThread.start();
			extractThread.join();
			extractThread = null;
			return AdapterExtractorContext.getInstance().getServerDomain();
		}
		catch (Exception e) {
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "采集失败:" + e.getLocalizedMessage());
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "采集结束时间:"
					+ FormatUtil.formatTimestamp(new Date()));
			e.printStackTrace(System.out);
			extractThread = null;
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote#auditLoad(String)
	 */
	public boolean auditLoad(String taskInstanceId) {
		if (extractThread != null) {
			// 只允许一个采集实例运行,如有多个进来(包括审核),则不采集
			throw new InadequateResourcesException("已有采集任务在运行中,其任务实例ID:"
					+ AdapterExtractorContext.getInstance().getTaskInstanceId());
		}
		
		// 缓存任务实例ID
		AdapterExtractorContext.getInstance().setTaskInstanceId(taskInstanceId);
		log.info("-------------------------------------\n审核入库开始运作...");
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO,
				"-------------------------------------\n审核入库开始运作...");

		AdapterExtractorContext.getInstance().genGlobalTime();
		try {
			IAuditService iAuditService = new AuditServiceImpl();
			return iAuditService.operate();
		} catch (Exception e) {
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "采集失败:" + e.getLocalizedMessage());
			e.printStackTrace(System.out);
		} finally {
			log.info("-------------------------------------审核入库执行结束...");
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "审核入库结束时间:"
					+ FormatUtil.formatTimestamp(new Date()));
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote
	 * #receiveServerHostAndPort(java.lang.String, java.lang.Integer)
	 */
	public void receiveServerHostAndPort(String host, Integer port) {
		ExtractorContextLoader.getRmiProxyFactoryBean().setServerHostAndPort(host, port);
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote
	 * #receiveJdbcProperties(java.util.Properties)
	 */
	public void receiveJdbcProperties(Properties properties) {
		BasicDataSource dataSource = ExtractorContextLoader.getDataSource();
		String driver = properties.getProperty("jdbc.driverClassName", "");
		String url = properties.getProperty("jdbc.url", "");
		String username = properties.getProperty("jdbc.username", "");
		String password = properties.getProperty("jdbc.password", "");
		
		boolean restartNeeded = false;
		if (!driver.equals(dataSource.getDriverClassName())) {
			dataSource.setDriverClassName(driver);
			restartNeeded = true;
		}
		if (!url.equals(dataSource.getUrl())) {
			dataSource.setUrl(url);
			restartNeeded = true;
		}
		if (!username.equals(dataSource.getUsername())) {
			dataSource.setUsername(username);
			restartNeeded = true;
		}
		if (!password.equals(dataSource.getPassword())) {
			dataSource.setPassword(password);
			restartNeeded = true;
		}
		
		if (restartNeeded) { //关闭旧的连接池
			try {
				dataSource.close();
			} catch (SQLException e) {
				log.error("restart dataSource error", e);
			}
			if (log.isInfoEnabled()) {
				StringBuffer sb = new StringBuffer();
				sb.append("We receive the Jdbc Connection Properties as these:\n")
				  .append("jdbc.driverClassName=").append(driver).append("\n")
				  .append("jdbc.url=").append(url).append("\n")
				  .append("jdbc.username=").append(username).append("\n")
				  .append("jdbc.password=******");
				log.info(sb.toString());
			}
			GenSqlUtil.restore();
		}
	}

	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.control.IExtractorRemote
	 * #testConnect(java.lang.String[], java.util.Properties)
	 */
	public boolean testConnect(String[] springFiles, Properties properties) throws TestConnectionException {
		String[] contextFiles = new String[springFiles.length];
		for (int i=0; i<springFiles.length; i++) {
			contextFiles[i] = Constants.SPRING_DIRECTORY + springFiles[i];
		}
		
		ApplicationContext aCtx = null;
		try {
			aCtx = AdapterContextLoader.createApplicationContext(contextFiles, properties);
			// 获取连接，并测试
			if (!aCtx.containsBean("testConnect")) {
				throw new TestConnectionException("该适配器没有定义testConnect");
			}
			ITestConnection tc = (ITestConnection)aCtx.getBean("testConnect");
			tc.testConnect();
		} catch (NestedRuntimeException e) {
			throw new TestConnectionException(e.getMostSpecificCause());
		} catch (Exception e) {
			throw new TestConnectionException(e);
		} finally {
			if (aCtx != null) {
				((AbstractApplicationContext)aCtx).destroy();
			}
		}
		return true;
	}

}
