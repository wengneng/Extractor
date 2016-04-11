package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.util.CellRangeAddress;

import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsConfig;
import com.pactera.edg.am.metamanager.core.util.POIExcelUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * ADMS采集器，采用PoiExcelTemplateMappingServiceImpl采集器方法，实现ADMS的SDM映射采集器
 * 
 * @author user
 * @version 1.0 Date: 5 30, 2013
 * 
 */
public class AdmsMappingServiceImpl extends PoiExcelTemplateMappingServiceImpl  implements IMetadataMappingService {

	private Log log = LogFactory.getLog(AdmsMappingServiceImpl.class);
	
	private  List<Map<String,String>> dataMap = new ArrayList<Map<String,String>>();
	
	private String sysName;
	
	//1：目标表英文名，2：目标字段英文名，5：目标表库名，6：目标表中文名，7：目标字段中文名
	//8：目标字段数据类型，11：目标字段赋值规则，13：主源字段英文名，14：主源字段中文名，
	//15：主源字段数据类型，17：主源表库名，18：主源表英文名
	//20：主源表中文名，21：JOIN方式，22：次源表库名，
	//23：次源表英文名，24：次源表别名，25：次源表中文名
	//26：JOIN条件，28：WHERE条件，33：备注
	private  int [] cellNum = new int[] {1,2,5,6,7,8,11,13,14,15,17,18,20,21,22,23,24,25,26,28,33};
	
	public void metadataMapping(AppendMetadata aMetadata) throws Exception {
		super.aMetadata = aMetadata;
		// 获取模板编号，取得模板配置
		IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance().getIClassifier();
		String dsId = AdapterExtractorContext.getInstance().getDatasourceId();
		List<TemplateClsConfig> tcConfigs = iClassifier.getTemplateClsConfigsByDs(dsId);

		// 获取模板编号，取得模板配置结束
		String path = AdapterExtractorContext.getInstance().getDsAbsolutePath();
		try {

			log.info(new StringBuilder("开始解析Excel文件,文件绝对路径:").append(path).append(" ,...").toString());
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, new StringBuilder("开始解析Excel文件,文件绝对路径:")
					.append(path).append(" ,...").toString());
			
			convertTemplate(path);
			
			super.iterTemplateConf(tcConfigs);

			super.setDependencies(tcConfigs);

			aMetadata.setChildMetaModels(aMetadata.getMetaModel()
					.getChildMetaModels());
			aMetadata.setDDependencies(super.dependencies);

			super.afterExtracted();

			super.printExceptionData();
			log.info("结束解析Excel文件 END...");
		}
		catch (IOException ce) {
			log.error("解析失败, 请确保该文件存在且为xls格式的文件，文件绝对路径:" + path, ce);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "解析失败, 请确保该文件存在且为xls格式的文件，文件绝对路径:" + path);
			throw ce;
		}
		catch (Exception e) {
			log.error("Excel文件解析失败, 文件绝对路径:" + path, e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, "Excel文件解析失败, 文件绝对路径:" + path);
			throw e;
		}
		finally {
			clear();
		}
	}
	
	public void clear() {
		super.wBook = null;
		super.aMetadata = null;
		super.dependencies = null;
		super.metaModelContext = null;

		super.mdContext = null;
		super.finishedTcConfigs.clear();
		// finishedMetaModels.clear();
		super.forefatherMetaModels.clear();

		super.metadataExceptionDataCache.clear();
		super.valueDependencyExceptionDataCache.clear();
		super.deleteDataBos.clear();
		super.deplicateDependency = null;
		super.dependencies = null;
	}
	
	
	/**
	  * 对SDM文件进行转换，转换格式采用POIExcelTemplate所使用的格式（文件格式参考Template采集适配器）
	  * @Title: admsConvertTemplate  
	  * @Description: TODO 
	  * @param @param path
	  * @param @return
	  * @param @throws FileNotFoundException
	  * @param @throws IOException 
	  * @return HSSFWorkbook 
	  * @throws
	 */
	public void convertTemplate(String path) throws Exception{
		log.info("开始从SDM转换映射采集所需数据文件   START...");
		//获取文件所上传路径的存放地址
		super.wBook = new HSSFWorkbook(new FileInputStream(new File(path)));
		try {
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		HSSFSheet sheet = (HSSFSheet) super.wBook.getSheetAt(0);
		//获取该sheet下面最后一行行数
		int rowCount = sheet.getLastRowNum();
		int num = 1;
		for(int j = 2 ; j <= rowCount ; j ++){
			HSSFRow row = sheet.getRow(j);
			//如果行为NULL，则continue
			if(row==null){
				continue;
			}
			//获取最有一列的列数
			int cellCount = row.getLastCellNum();
			if(cellCount<=0){
				continue;
			}
			HSSFCell cell = row.getCell(13);
			//踢出主源字段英文名为空的行
			if(cell==null){
				continue;
			}else{
				String value = cell.getRichStringCellValue().toString();
				//主源字段不允许为空
				if(value == null || value.trim().equals("")){
					continue;
				}
			}
			Map<String,String> map = new HashMap<String, String>();
			//获取已固定的列值
			for(int k = 0 ; k < cellNum.length ; k ++){
				try {
					cell = row.getCell(cellNum[k]);
					if(cell==null)
						continue;
					map.put(""+cellNum[k], POIExcelUtil.getCellValue(cell));
				} catch (Exception e) {
					e.printStackTrace();
					throw new Exception("获取SDM值出现异常，异常列数为："+cellNum[k]+"，行数为："+j+"，错误信息为："+e.getMessage()+"，请检查...");
				}
			}
			dataMap.add(analyDataMap(map,num++,j));
		}
		createSheetAndRowValue();
		log.info("结束从SDM转换映射采集所需数据文件  END...");
	}
	/**
	  * 生成Template采集适配器所需的XLS文件
	  * @Title: createSheetAndRowValue  
	  * @Description: TODO 
	  * @param  
	  * @return void 
	  * @throws
	 */
	private  void createSheetAndRowValue() {
		super.wBook = null;
		super.wBook = new HSSFWorkbook();
		HSSFSheet sheet = (HSSFSheet) super.wBook.createSheet();
		// 设置Title
		HSSFRow row = sheet.createRow(0);
		HSSFCell cell = row.createCell(0);
		// 四个参数分别是：起始行，结束行，起始列，结束列   
		//转换
		sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 1));
		cell = row.createCell(0);
		cell.setCellValue(new HSSFRichTextString("转换"));
		//目标系统
		sheet.addMergedRegion(new CellRangeAddress(0, 0, 2, 8));
		cell = row.createCell(2);
		cell.setCellValue(new HSSFRichTextString("目标系统"));
		//源系统
		sheet.addMergedRegion(new CellRangeAddress(0, 0, 9, 17));
		cell = row.createCell(9);
		cell.setCellValue(new HSSFRichTextString("源系统"));
		//关注信息修改日期
		sheet.addMergedRegion(new CellRangeAddress(0, 1, 18, 18));
		cell = row.createCell(18);
		cell.setCellValue(new HSSFRichTextString("关注信息修改日期"));
		//序号
		sheet.addMergedRegion(new CellRangeAddress(0, 1, 19, 19));
		cell = row.createCell(19);
		cell.setCellValue(new HSSFRichTextString("序号"));
		
		// 设置Title
		row = sheet.createRow(1);
		cell = row.createCell(0);
		cell.setCellValue(new HSSFRichTextString("程序名"));
		cell = row.createCell(1);
		cell.setCellValue(new HSSFRichTextString("程序中文名"));
		cell = row.createCell(2);
		cell.setCellValue(new HSSFRichTextString("目标库"));
		cell = row.createCell(3);
		cell.setCellValue(new HSSFRichTextString("目标库中文名"));
		cell = row.createCell(4);
		cell.setCellValue(new HSSFRichTextString("目标表"));
		cell = row.createCell(5);
		cell.setCellValue(new HSSFRichTextString("目标表中文名"));
		cell = row.createCell(6);
		cell.setCellValue(new HSSFRichTextString("目标字段名"));
		cell = row.createCell(7);
		cell.setCellValue(new HSSFRichTextString("目标字段中文名"));
		cell = row.createCell(8);
		cell.setCellValue(new HSSFRichTextString("目标字段数据类型"));
		cell = row.createCell(9);
		cell.setCellValue(new HSSFRichTextString("源库"));
		cell = row.createCell(10);
		cell.setCellValue(new HSSFRichTextString("源库中文名"));
		cell = row.createCell(11);
		cell.setCellValue(new HSSFRichTextString("源表"));
		cell = row.createCell(12);
		cell.setCellValue(new HSSFRichTextString("源表中文名"));
		cell = row.createCell(13);
		cell.setCellValue(new HSSFRichTextString("源字段名"));
		cell = row.createCell(14);
		cell.setCellValue(new HSSFRichTextString("源字段中文名"));
		cell = row.createCell(15);
		cell.setCellValue(new HSSFRichTextString("源字段数据类型"));
		cell = row.createCell(16);
		cell.setCellValue(new HSSFRichTextString("字段映射表达式"));
		cell = row.createCell(17);
		cell.setCellValue(new HSSFRichTextString("备注"));
		
		for (int i = 0 ; i < dataMap.size() ; i++) {
			Map<String,String> data = dataMap.get(i);
			if(data==null)
				continue;
			HSSFRow row1 = sheet.createRow(i+2);
			cell = row1.createCell(0);
			//程序名称
			cell.setCellValue(new HSSFRichTextString(sysName));
			cell = row1.createCell(1);
			//程序中文名称
			cell.setCellValue(new HSSFRichTextString(sysName));
			//1：目标表英文名，2：目标字段英文名，5：目标表库名，6：目标表中文名，7：目标字段中文名
			//8：目标字段数据类型，11：目标字段赋值规则，13：主源字段英文名，14：主源字段中文名，
			//15：主源字段数据类型，17：主源表库名，18：主源表英文名
			//20：主源表中文名，21：JOIN方式，22：次源表库名，
			//23：次源表英文名，24：次源表别名，25：次源表中文名
			//26：JOIN条件，28：WHERE条件，33：备注
			
			//目标库
			cell = row1.createCell(2);
			cell.setCellValue(new HSSFRichTextString(data.get("5").substring(2,data.get("5").lastIndexOf("}"))));
			//目标库中文名
			cell = row1.createCell(3);
			cell.setCellValue(new HSSFRichTextString(data.get("5").substring(2,data.get("5").lastIndexOf("}"))));
			//目标表
			cell = row1.createCell(4);
			cell.setCellValue(new HSSFRichTextString(data.get("1")));
			//目标表中文名
			cell = row1.createCell(5);
			cell.setCellValue(new HSSFRichTextString(data.get("6")));
			//目标字段名
			cell = row1.createCell(6);
			cell.setCellValue(new HSSFRichTextString(data.get("2")));
			//目标字段中文名
			cell = row1.createCell(7);
			cell.setCellValue(new HSSFRichTextString(data.get("7")));
			//目标字段数据类型
			cell = row1.createCell(8);
			cell.setCellValue(new HSSFRichTextString(data.get("8")));
			//源库
			cell = row1.createCell(9);
			cell.setCellValue(new HSSFRichTextString(data.get("17").substring(2,data.get("17").lastIndexOf("}"))));
			//源库中文名
			cell = row1.createCell(10);
			cell.setCellValue(new HSSFRichTextString(data.get("17").substring(2,data.get("17").lastIndexOf("}"))));
			//源表
			cell = row1.createCell(11);
			cell.setCellValue(new HSSFRichTextString(data.get("18")));
			//源表中文名
			cell = row1.createCell(12);
			cell.setCellValue(new HSSFRichTextString(data.get("20")));
			//源字段名
			cell = row1.createCell(13);
			cell.setCellValue(new HSSFRichTextString(data.get("13")));
			//源字段中文名
			cell = row1.createCell(14);
			cell.setCellValue(new HSSFRichTextString(data.get("14")));
			//源字段数据类型
			cell = row1.createCell(15);
			cell.setCellValue(new HSSFRichTextString(data.get("15")));
			//字段映射表达式
			cell = row1.createCell(16);
			cell.setCellValue(new HSSFRichTextString(data.get("99")));
			//备注
			cell = row1.createCell(17);
			cell.setCellValue(new HSSFRichTextString(data.get("33")));
			//关注信息修改日期
			cell = row1.createCell(18);
			cell.setCellValue(new HSSFRichTextString(data.get("97")));
			//序号
			cell = row1.createCell(19);
			cell.setCellValue(new HSSFRichTextString(data.get("98")));
		}
	}
	
	/**
	 * @throws Exception 
	  * 解析获取的SDM映射数据，通过制定的规则踢除以及调整相对应的值
	  * @Title: analyDataMap  
	  * @Description: TODO 
	  * @param @param data 
	  * @return void 
	  * @throws
	 */
	private  Map<String,String> analyDataMap(Map<String,String> data,int num,int j) throws Exception{
		try {
			//13：主源字段英文名
			String oColumn = data.get("13");
			//11：目标字段赋值规则
			String tColumnRule = data.get("11");
			//目标字段赋值规则不允许为null并且主源字段如果在规则中存在则保留否则剔除
			if(tColumnRule!=null&&tColumnRule.toUpperCase().indexOf(oColumn.toUpperCase())!=-1){
				//21：JOIN方式
				String joinW = data.get("21");
				String asName = data.get("24");
				//判断是否存在次源表关联情况
				if(joinW!=null&&!joinW.trim().equals("")
						&&tColumnRule.toUpperCase().indexOf(asName.toUpperCase()+".")!=-1){
					//将主源表英文名替换成次源表英文名
					data.put("18", data.get("23"));
					//将主源表中文名替换成次源表中文名
					data.put("20", data.get("25"));
					//21：JOIN方式，22：次源表库名，
					//23：次源表英文名，24：次源表别名，25：次源表中文名
					//26：JOIN条件，28：WHERE条件
					//99：字段映射表达式
					data.put("99", data.get("21")+" "+data.get("23")+" "+data.get("24")+" on "+ data.get("26")+" where 1=1 "+data.get("28") != null && !data.get("28").trim().equals("") ? "AND "+data.get("28") : "" );
				}else{
					data.put("99", "");
				}
				//关注信息修改日期
				data.put("97", new SimpleDateFormat("yyyy/MM/dd").format(new Date()));
				//序号
				data.put("98",""+num);
				data.remove("21");
				data.remove("23");
				data.remove("24");
				data.remove("25");
				data.remove("26");
				data.remove("28");
				if(sysName==null||sysName.trim().equals("")){
					sysName = data.get("18").substring(0,data.get("18").indexOf("_"));
				}
				return data;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("执行解析SDM时出现异常，异常行数为："+j+"，错误信息为："+e.getMessage()+"，请检查...");
		}
			
			return null;
	}
	
	public static void main(String[] args) throws Exception {
		AdmsMappingServiceImpl a = new AdmsMappingServiceImpl();
		a.convertTemplate("G:\\1：Vanceinfo\\ADMS\\SDB_EDW_sdmTaskDetails_S59国债系统.xls");
	}
	
}
