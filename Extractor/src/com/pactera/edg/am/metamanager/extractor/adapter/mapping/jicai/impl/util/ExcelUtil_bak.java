package com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util;

/**    
  * @FileName: ExcelUtil.java  
  * @Package:com.vibi.dgp.extractor.adapter.mapping  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2012-11-21 上午10:22:17  
  * @version V1.0    
  */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsConfig;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleJoint;

/** 
  * @ClassName: ExcelUtil  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2012-11-21 上午10:22:17   
  */
public class ExcelUtil_bak {
	
	// 自底向上，已经处理过的元模型缓存于此
	public Set<TemplateClsConfig> finishedTcConfigs = new HashSet<TemplateClsConfig>();
	// 标题开始行，标题结束行，数据开始行,数据结束行
	private int tStart, tEnd, dStart, dEnd;
	
	private HSSFWorkbook wbook;
	
	/** 元素的类型，分别为元模型的代码，名称，祖先和属性
	 * 
	 * @author user
	 * @version 1.0 Date: Dec 27, 2010
	 * 
	 */
	private enum NodeType {
		CODE, NAME, FOREPARENT, ATTRIBUTE
	}
	
	/**
	 * 作为元模型的上下文，包括元模型名称，所在SHEET，及元数据CODE/NAME所在SHEET的位置；
	 * 属性所在SHEET的位置；祖先结点CODE所在SHEET的位置 注意：该上下文只对当前SHEET适用，不重用于下一个SHEET
	 * 
	 * @author user
	 * @version 1.0 Date: Dec 22, 2010
	 * 
	 */
	private class MetaModelContext {

		public static final String CODE_FLAG = "==CODE==", NAME_FLAG = "==NAME==";

		Map<String, int[]> metaPositionMapping = new HashMap<String, int[]>(2);

		Map<String, int[]> parentPositionMapping = new LinkedHashMap<String, int[]>(0);
	}
	
	/**
	 * @throws IOException 
	 * @throws FileNotFoundException 
	  * 优化Excel填写规范，统一对Excel模板填写内容进行大写转换
	  * （华润需求）
	  * @Title: ExeclValueToUpperCase  
	  * @Description: TODO 
	  * @param @param path
	  * @param @return 
	  * @return HSSFWorkbook 
	  * @throws
	 */
	public HSSFWorkbook ExeclValueToUpperCase(String path,List<TemplateClsConfig> tcConfigs) throws FileNotFoundException, IOException{
		//获取文件所上传路径的存放地址
		this.wbook = new HSSFWorkbook(new FileInputStream(new File(path)));
		iterTemplateConf(tcConfigs);
		return this.wbook;
	}
	private void iterTemplateClsTitle(TemplateClsTitle tcTitle){
		
		// 元数据代码
		List<TemplateTitleJoint> joints = tcTitle.getMdCode().getTitleJoints();
		
		for(int t = 0 ; t < joints.size() ; t ++){
			TemplateTitleJoint tj = joints.get(t);
			List<TemplateTitle> titles = tj.getTitles();
			for(int u = 0 ; u < titles.size() ; u++){
				TemplateTitle title = titles.get(u);
				String titleName = title.getName();
			}
		}
		
		
		
		//sheet长度
		int sheetCount = this.wbook.getNumberOfSheets();
		//循环获取sheet，并对sheet内容进行大小写转换
		for(int i = 2 ; i < sheetCount ; i ++ ){
			HSSFSheet sheet = this.wbook.getSheetAt(i);
			//获取该sheet下面最后一行行数
			int rowCount = sheet.getLastRowNum();
			for(int j = 1 ; j < rowCount ; j ++){
				HSSFRow row = sheet.getRow(j);
				//如果行为NULL，则continue
				if(row==null){
					continue;
				}
				//获取最有一列的列数
				int cellCount = row.getLastCellNum();
				int cellNum [] = null; 
				if(i==2){
					cellNum = new int[] {2,3};
				}else if (i==3){
					cellNum = new int[] {2,3,6,8};
				} else if ( i ==4 ){
					cellNum = new int[] {2,3,5,7};
				}
				
				for(int k = 0 ; k < cellNum.length ; k ++){
					HSSFCell cell = row.getCell(cellNum[k]);
					if(cell==null)
						continue;
					//当前cell值，必须为string类型
					if(cell.getCellType()==cell.CELL_TYPE_STRING){
						//获取当前cell值
						String val = cell.getRichStringCellValue().toString();
						cell.setCellValue(val.replaceAll(" ", "").toUpperCase());
						System.out.println("Excle列值为转换前："+val+" ；转换后："+cell.getRichStringCellValue().toString());
					}
				}
			}
		}
	}
	
	/**
	 * 自顶向下根据元模型的树型结构从根开始递归遍历，当遇到元模型为叶子结点时，再自底向上遍历处理每一个元模型
	 * 
	 * @param tcConfigs
	 */
	public void iterTemplateConf(List<TemplateClsConfig> tcConfigs) {
		for (TemplateClsConfig tcConfig : tcConfigs) {

			if (tcConfig.getCompeds().size() == 0) {
				// 自底向上，开始干活
				iterParentTConf(tcConfig);
			}
			else {
				// 组合关系，递归向下
				iterTemplateConf(tcConfig.getCompeds());
			}
		}
	}
	
	/**
	 * 自底向上遍历处理每一个元模型配置
	 * 
	 * @param tcConfig
	 */
	private void iterParentTConf(TemplateClsConfig tcConfig) {
		if (tcConfig == null) // 根即为没有父结点的配置
			return;
		if (finishedTcConfigs.contains(tcConfig)) // 自底向上，遇到已经处理过的则返回
			return;
		List<TemplateClsTitle> tcTitles = tcConfig.getClsTitles();
		for (int index = tcTitles.size() - 1; index >= 0; index--) {
			// 一个元模型的其中一个配置
			// 先处理后面的，再处理前面的，原因在于对于相同数据，后处理的会覆盖先处理的，为了保证数据是第一个配置的，故使用倒序处理
			TemplateClsTitle tcTitle = tcTitles.get(index);
			cleanConf();
			iterTemplateClsTitle(tcTitle);
		}
		finishedTcConfigs.add(tcConfig);
		iterParentTConf(tcConfig.getParentTcConfig());
	}
	private void cleanConf() {
		dStart = -1;
		dEnd = -1;
		tStart = -1;
		tEnd = -1;
	}
	public static void main(String[] args) {
		ExcelUtil_bak e = new ExcelUtil_bak();
		try {
//			e.ExeclValueToUpperCase("D:\\华润银行ODS一期项目_数据字典_A102_UM_用户管理.xls");
		} catch (Exception e2) {
			// TODO: handle exception
			e2.printStackTrace();
		}
	}
}	
