/**    
  * @FileName: HsqlDBUtil.java  
  * @Package:com.vibi.dgp.extractor.adapter.cgb.extract.util  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2012-10-8 下午5:14:15  
  * @version V1.0    
  */
package com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.impl.AbstractJiCaiDBExtractService;

/** 
  * @ClassName: HsqlDBUtil  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2012-10-8 下午5:14:15   
  */
public class HSqlDB {
	private static Log log = LogFactory.getLog(HSqlDB.class);
	private static Connection connection = null;
	private static boolean flag = false;
	private final static String cgb_properties = "extractor/cgb_extractor.properties";
	public static Properties cgbPro;

	private static void setConnection(){
		try {
			Class.forName("org.hsqldb.jdbcDriver");
			connection = DriverManager.getConnection(
					getCgbPro().getProperty("hsql_connection"), "sa", "");
			flag = true;
			log.info("开启内存数据库，数据库状态为："+flag);
		} catch (Exception e) {
			e.printStackTrace();
			log.info("在初始化hsqldb数据库时，出现" + e.getMessage() + "错误！");
		}
	}
	private static void initProperties() throws ServletException {
		InputStream in = null;
		cgbPro = new Properties();
		try {
			in = HSqlDB.class.getClassLoader()
					.getResourceAsStream(cgb_properties);
			cgbPro.load(in);
		} catch (IOException e) {
			throw new ServletException(e);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**  
	  * @return connection  
	 * @throws ServletException 
	  */
	public static Properties getCgbPro() throws ServletException {
		if( cgbPro == null){
			initProperties();
			return cgbPro;
		}
		else
			return cgbPro;
	}
	/**  
	  * @return connection  
	  */
	public static Connection getConnection() {
		if( connection == null || flag == false){
			setConnection();
			return connection;
		}
		else
			return connection;
	}

	public static void Shutdown() throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = getConnection().prepareStatement("SHUTDOWN");
			ps.executeUpdate();
			flag = false;
			log.info("关闭hsqldb内存数据库，数据库状态为："+flag);
		} catch (Exception e) {
			e.printStackTrace();
			log.info("关闭hsqldb内存数据库时出现" + e.getMessage() + "错误！");
		} finally {
			if (ps != null) {
				ps.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}
}
