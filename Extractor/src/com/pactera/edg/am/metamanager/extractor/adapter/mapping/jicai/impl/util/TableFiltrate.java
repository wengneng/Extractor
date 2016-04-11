/**    
  * @FileName: TableFilter.java  
  * @Package:com.vibi.dgp.extractor.adapter.cgb.mapping.util  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2013-1-8 下午9:51:28  
  * @version V1.0    
  */
package com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SchemaVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SystemVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;

/** 
  * @ClassName: TableFilter  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2013-1-8 下午9:51:28   
  */
public class TableFiltrate {
	private Log log = LogFactory.getLog(this.getClass());
	private static Properties pro;
	private final static String table_filter = "extractor/cgb_extractor.properties";
	private Map<String, String> tableMap = null;
	private static TableFiltrate filtrate = null;

	public static TableFiltrate getInstance() {
		if (filtrate == null) {
			filtrate = new TableFiltrate();
		}
		return filtrate;
	}

	/**
	 * @throws IOException 
	 * @throws FileNotFoundException 
	  * 对数据进行过滤
	  * @Title: filtrate  
	  * @Description: TODO 
	  * @param @param sysInfos 
	  * @return void 
	  * @throws
	 */
	public void filtrate(SystemVO sysInfos) throws IOException {
		//初始化配置文件
		init();
		//读取已配置过滤文件路径
		String path = (String) pro.get("t.tab.filtrate.name.path");
		if (path == null) {
			log.info("【表过滤】过滤表文件路径无配置，不对表进行过滤！");
			return;
		}
		File f = new File(path);
		//如果文件不存在，不再继续操作
		if (!f.exists()) {
			log.info("【表过滤】过滤文件不存在：" + path);
			return;
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(f));
			String tableName = null;
			int line = 1;
			tableMap = new HashMap<String, String>();
			//一次读入一行，直到读入null为文件结束 
			while ((tableName = reader.readLine()) != null) {
				//表名，行号
				tableMap.put(tableName.toUpperCase(), "" + line);
				line++;
			}
			reader.close();
			//当文件中不写入需筛选表名时，则默认不筛选
			if(tableMap.size()==0){
				log.info("【表过滤】过滤文件中内容为空！");
				return;
			}
			//筛选过滤表
			filtrateName(sysInfos);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			log.info("【表过滤】过滤文件不存在：" + e.getMessage());
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.info("【表过滤】读取过滤文件出错：" + e.getMessage());
			throw e;
		} finally {
			if (reader != null)
				reader.close();
			if (tableMap != null)
				tableMap.clear();
		}
	}

	/**
	  * 开始对表名进行过滤
	  * @Title: filtrateName  
	  * @Description: TODO 
	  * @param @param sysInfos 
	  * @return void 
	  * @throws
	 */
	private void filtrateName(SystemVO sysInfos) {
		List<SchemaVO> schema = sysInfos.getSchemas();
		List<SchemaVO> sList = new ArrayList<SchemaVO>();
		//迭代已采集数据字典，对数据进行过滤筛选
		for (SchemaVO schemaVO : schema) {
			//列
			List<ColumnVO> cList = new ArrayList<ColumnVO>();
			//表
			List<TableVO> tList = new ArrayList<TableVO>();
			SchemaVO svo = new SchemaVO();
			List<ColumnVO> column = schemaVO.getFields();
			int count = 1;
			//对列数据字典过滤筛选
			for (ColumnVO columnVO : column) {
				//当文件中存在该表，则对该表数据进行保存，其它则弃之
				if (tableMap.containsKey(columnVO.geteTableName().toUpperCase())) {
					cList.add(columnVO);
					continue;
				}
				count++;
			}
			log.info("【表过滤】"+schemaVO.getSchName()+"：column 总记录数为："+column.size()+"，剔除量为："+count);
			count = 1;
			List<TableVO> table = schemaVO.getTables();
			//对表数据字典过滤筛选
			for (TableVO tableVO : table) {
				//对列数据字典过滤筛选
				//当文件中存在该表，则对该表数据进行保存，其它则弃之
				if (tableMap.containsKey(tableVO.geteTableName().toUpperCase())) {
					tList.add(tableVO);
					continue;
				}
				count++;
			}
			log.info("【表过滤】"+schemaVO.getSchName()+"：table 总记录数为："+table.size()+"，剔除量为："+count);
			svo.setSchName(schemaVO.getSchName());
			svo.setFields(cList);
			svo.setTables(tList);
			sList.add(svo);
		}
		//保存到字典中
		sysInfos.setSchemas(sList);
	}

	private void init() {
		InputStream in = null;
		pro = new Properties();
		try {
			in = this.getClass().getClassLoader()
					.getResourceAsStream(table_filter);
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
