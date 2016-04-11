/**    
  * @FileName: PropertiesUtil.java  
  * @Package:com.vibi.cgb.edw.ahq.soap.bieews  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2012-12-30 下午4:16:36  
  * @version V1.0    
  */
package com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** 
  * @ClassName: PropertiesUtil  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2012-12-30 下午4:16:36   
  */
public class PropertiesUtil {
	
	private final static String cgb_properties = "extractor/cgb_extractor.properties";
	public static Properties pro;
	static{
		PropertiesUtil u = new PropertiesUtil();
		u.init();
	}
	
	public void init(){
		 InputStream in = null;
			pro = new Properties();
			try {
				in = this.getClass().getClassLoader()
						.getResourceAsStream(cgb_properties);
				pro.load(in);
			} catch (IOException e) {
				e.printStackTrace();
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
}
