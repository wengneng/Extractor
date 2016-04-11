/**  
 * 文件名：ExcelUtil.java   
*   
* 版本信息：   
* 日期：2012-8-28   
* Copyright 足下 Corporation 2012    
* 版权所有   
*   
*/
package com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.ColumnVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SchemaVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SystemVO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.TableVO;

/**  
 * 元数据定制
 *    
* 项目名称：metamanager   
* 类名称：ExcelUtil   
* 类描述：   
* 创建人：kaishui   
* 创建时间：2012-8-28 下午3:57:36   
* 修改人：kaishui   
* 修改时间：2012-8-28 下午3:57:36   
* 修改备注：   
* @version    
*    
*/
public class JiCaiExcelUtil {
	private Log log = LogFactory.getLog(JiCaiExcelUtil.class);

	public static JiCaiExcelUtil init() {
		return new JiCaiExcelUtil();
	}

	/**
	 * @throws IOException 
	 * @throws FileNotFoundException 
	  * 优化Excel填写规范，统一对Excel模板填写内容进行大写转换
	  * （华润需求），未动态获取列值
	  * @Title: ExeclValueToUpperCase  
	  * @Description: TODO 
	  * @param @param path
	  * @param @return 
	  * @return HSSFWorkbook 
	  * @throws
	 */
	public HSSFWorkbook ExeclValueToUpperCase(String path) throws FileNotFoundException, IOException{
		//获取文件所上传路径的存放地址
		HSSFWorkbook wbook = new HSSFWorkbook(new FileInputStream(new File(path)));
		//sheet长度
		int sheetCount = wbook.getNumberOfSheets();
		//循环获取sheet，并对sheet内容进行大小写转换
		for(int i = 2 ; i < sheetCount ; i ++ ){
			HSSFSheet sheet = wbook.getSheetAt(i);
			//获取该sheet下面最后一行行数
			int rowCount = sheet.getLastRowNum();
			for(int j = 1 ; j <= rowCount ; j ++){
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
				int cellNum [] = null; 
				if(i==2){
					cellNum = new int[] {2,3};
				}else if (i==3){
					cellNum = new int[] {2,3,6,8};
				} else if ( i ==4 ){
					cellNum = new int[] {2,3,5,7};
				}else{
					cellNum = new int[]{};
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
//						System.out.println(i+"--Excle列值为转换前："+val+" ；转换后："+cell.getRichStringCellValue().toString());
					}
				}
			}
		}
		return wbook;
	}
	/**
	 *  创建系统级ROW信息
	  * @Title: createSheet3Row  
	  * @Description: TODO 
	  * @param @param sheet 
	  * @return void 
	  * @throws
	 */
	private void createSheet3Row(HSSFSheet sheet, SystemVO systemVO) {
		try {
			// 设置系统级Title
			HSSFRow row = sheet.createRow(0);
			HSSFCell cell = row.createCell(0);
			cell.setCellValue(new HSSFRichTextString("系统"));
			cell = row.createCell(1);
			cell.setCellValue(new HSSFRichTextString("系统代码"));
			cell = row.createCell(2);
			cell.setCellValue(new HSSFRichTextString("功能简介"));
			cell = row.createCell(3);
			cell.setCellValue(new HSSFRichTextString("开发商"));
			cell = row.createCell(4);
			cell.setCellValue(new HSSFRichTextString("开发商联系人"));
			cell = row.createCell(5);
			cell.setCellValue(new HSSFRichTextString("行方经办人"));

			HSSFRow row1 = sheet.createRow(1);
			cell = row1.createCell(0);
			//填入系统名称
			cell.setCellValue(new HSSFRichTextString(systemVO.getSysName()));
			cell = row1.createCell(1);
			//系统英文名称
			cell.setCellValue(new HSSFRichTextString(systemVO.geteSysName()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createSheet4TopRow(HSSFSheet sheet) {
		// 设置Title
		HSSFRow row = sheet.createRow(0);
		HSSFCell cell = row.createCell(0);
		cell.setCellValue(new HSSFRichTextString("系统名称"));
		cell = row.createCell(1);
		cell.setCellValue(new HSSFRichTextString("系统代码"));
		cell = row.createCell(2);
		cell.setCellValue(new HSSFRichTextString("系统模块"));
		cell = row.createCell(3);
		cell.setCellValue(new HSSFRichTextString("表英文名"));
		cell = row.createCell(4);
		cell.setCellValue(new HSSFRichTextString("表中文名"));
		cell = row.createCell(5);
		cell.setCellValue(new HSSFRichTextString("描述"));
		cell = row.createCell(6);
		cell.setCellValue(new HSSFRichTextString("表类型"));
		cell = row.createCell(7);
		cell.setCellValue(new HSSFRichTextString("是否存在主键"));
		cell = row.createCell(8);
		cell.setCellValue(new HSSFRichTextString("重要程度"));
		cell = row.createCell(9);
		cell.setCellValue(new HSSFRichTextString("唯一索引"));
		cell = row.createCell(10);
		cell.setCellValue(new HSSFRichTextString("非唯一索引"));
	}

	/**
	 *  创建表级ROW信息
	  * @Title: createSheet4Row  
	  * @Description: TODO 
	  * @param @param sheet 
	  * @return void 
	  * @throws
	 */
	private void createSheet4Row(HSSFSheet sheet, List<TableVO> tables) {
		try {
			HSSFCell cell = null;
			//用于存储table行数
			int num = 1;
			for (TableVO tableVO : tables) {
				HSSFRow row1 = sheet.createRow(num);
				//填入系统名称
				cell = row1.createCell(0);
				cell.setCellValue(new HSSFRichTextString(tableVO.getSysName()));
				//系统英文名称
				cell = row1.createCell(1);
				cell.setCellValue(new HSSFRichTextString(tableVO.geteSysName()));
				//系统模块
				cell = row1.createCell(2);
				cell.setCellValue(new HSSFRichTextString(tableVO.getDbName()));
				//表英文名
				cell = row1.createCell(3);
				cell.setCellValue(new HSSFRichTextString(tableVO
						.geteTableName()));
				//表中文名
				cell = row1.createCell(4);
				cell.setCellValue(new HSSFRichTextString(tableVO.getTableName()));
				//描述
				cell = row1.createCell(5);
				cell.setCellValue(new HSSFRichTextString(tableVO.getDesc()));
				//表类型
				cell = row1.createCell(6);
				cell.setCellValue(new HSSFRichTextString(tableVO.getTableType()));
				//重要程度 是否存在主键
				cell = row1.createCell(7);
				cell.setCellValue(new HSSFRichTextString(tableVO.getIsPk()));
				//重要程度
				cell = row1.createCell(8);
				cell.setCellValue(new HSSFRichTextString(""));
				//唯一索引
				cell = row1.createCell(9);
				cell.setCellValue(new HSSFRichTextString(tableVO
						.getUniqueIndexName()));
				//非唯一索引
				cell = row1.createCell(10);
				cell.setCellValue(new HSSFRichTextString(tableVO
						.getNonuniqueIndexName()));
				num++;
			}
			tables.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createSheet5TopRow(HSSFSheet sheet) {
		// 设置Title
		HSSFRow row = sheet.createRow(0);
		HSSFCell cell = row.createCell(0);
		cell.setCellValue(new HSSFRichTextString("系统名称"));
		cell = row.createCell(1);
		cell.setCellValue(new HSSFRichTextString("系统代码"));
		cell = row.createCell(2);
		cell.setCellValue(new HSSFRichTextString("系统模块"));
		cell = row.createCell(3);
		cell.setCellValue(new HSSFRichTextString("表英文名"));
		cell = row.createCell(4);
		cell.setCellValue(new HSSFRichTextString("表中文名"));
		cell = row.createCell(5);
		cell.setCellValue(new HSSFRichTextString("字段序号"));
		cell = row.createCell(6);
		cell.setCellValue(new HSSFRichTextString("字段英文名"));
		cell = row.createCell(7);
		cell.setCellValue(new HSSFRichTextString("字段中文名"));
		cell = row.createCell(8);
		cell.setCellValue(new HSSFRichTextString("字段类型"));
		cell = row.createCell(9);
		cell.setCellValue(new HSSFRichTextString("主键"));
		cell = row.createCell(10);
		cell.setCellValue(new HSSFRichTextString("是否允许空值"));
		cell = row.createCell(11);
		cell.setCellValue(new HSSFRichTextString("是否代码字段"));
		cell = row.createCell(12);
		cell.setCellValue(new HSSFRichTextString("引用代码表"));
		cell = row.createCell(13);
		cell.setCellValue(new HSSFRichTextString("字段注释"));
		cell = row.createCell(14);
		cell.setCellValue(new HSSFRichTextString("问题/备注"));
	}

	/**
	 *  创建字段级ROW信息说明
	  * @Title: createSheet5Row  
	  * @Description: TODO 
	  * @param @param sheet 
	  * @return void 
	  * @throws
	 */
	private void createSheet5Row(HSSFSheet sheet, List<ColumnVO> fields) {
		try {
			HSSFCell cell = null;
			int num = 1;
			for (ColumnVO field : fields) {
				HSSFRow row1 = sheet.createRow(num);
				//填入系统名称
				cell = row1.createCell(0);
				cell.setCellValue(new HSSFRichTextString(field.getSysName()));
				//系统英文名称
				cell = row1.createCell(1);
				cell.setCellValue(new HSSFRichTextString(field.geteSysName()));
				//系统模块
				cell = row1.createCell(2);
				cell.setCellValue(new HSSFRichTextString(field.getDbName()));
				//表英文名
				cell = row1.createCell(3);
				cell.setCellValue(new HSSFRichTextString(field.geteTableName()));
				//表中文名
				cell = row1.createCell(4);
				cell.setCellValue(new HSSFRichTextString(field.getTableName()));
				//字段序号
				cell = row1.createCell(5);
				cell.setCellValue(new HSSFRichTextString(field.getFieldOrd()));
				//字段英文名
				cell = row1.createCell(6);
				cell.setCellValue(new HSSFRichTextString(field.geteFieldName()));
				//字段中文名
				cell = row1.createCell(7);
				int i = -1;
				if(field.getFieldName()!=null)
					i = field.getFieldName().indexOf("{{");
				if(i>=0){
					cell.setCellValue(new HSSFRichTextString(field.getFieldName().substring(i)));
				}else{
					cell.setCellValue(new HSSFRichTextString(field.getFieldName()));
				}
				//字段类型
				cell = row1.createCell(8);
				cell.setCellValue(new HSSFRichTextString(field.getFieldType()));
				//主键
				cell = row1.createCell(9);
				cell.setCellValue(new HSSFRichTextString(field.getKeyFlag()));
				//是否允许空值
				cell = row1.createCell(10);
				cell.setCellValue(new HSSFRichTextString(field.getNullFlag()));
				//是否代码字段
				cell = row1.createCell(11);
				cell.setCellValue(new HSSFRichTextString(""));
				//引用代码表
				cell = row1.createCell(12);
				cell.setCellValue(new HSSFRichTextString(""));
				//字段注释
				cell = row1.createCell(13);
				cell.setCellValue(new HSSFRichTextString(""));
				//问题/备注
				cell = row1.createCell(14);
				cell.setCellValue(new HSSFRichTextString(""));
				num++;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 *  创建公共落地代码Title
	  * @Title: createSheet6Row  
	  * @Description: TODO 
	  * @param @param sheet 
	  * @return void 
	  * @throws
	 */
	private void createSheet6Row(HSSFSheet sheet) {
		// 设置Title
		HSSFRow row = sheet.createRow(0);
		HSSFCell cell = row.createCell(0);
		cell.setCellValue(new HSSFRichTextString("系统名称"));
		cell = row.createCell(1);
		cell.setCellValue(new HSSFRichTextString("英文缩写"));
		cell = row.createCell(2);
		cell.setCellValue(new HSSFRichTextString("表名称"));
		cell = row.createCell(3);
		cell.setCellValue(new HSSFRichTextString("表中文名称"));
		cell = row.createCell(4);
		cell.setCellValue(new HSSFRichTextString("代码取值"));
		cell = row.createCell(5);
		cell.setCellValue(new HSSFRichTextString("代码描述"));
		cell = row.createCell(6);
		cell.setCellValue(new HSSFRichTextString("是否虚拟表"));
		cell = row.createCell(7);
		cell.setCellValue(new HSSFRichTextString("备注"));
	}

	/** 
	 *  创建字段落地代码Title
	  * @Title: createSheet7TopRow  
	  * @Description: TODO 
	  * @param @param sheet 
	  * @return void 
	  * @throws
	 */
	private void createSheet7TopRow(HSSFSheet sheet) {
		// 设置Title
		HSSFRow row = sheet.createRow(0);
		HSSFCell cell = row.createCell(0);
		cell.setCellValue(new HSSFRichTextString("系统名称"));
		cell = row.createCell(1);
		cell.setCellValue(new HSSFRichTextString("系统代码"));
		cell = row.createCell(2);
		cell.setCellValue(new HSSFRichTextString("系统模块"));
		cell = row.createCell(3);
		cell.setCellValue(new HSSFRichTextString("表英文名"));
		cell = row.createCell(4);
		cell.setCellValue(new HSSFRichTextString("表中文名"));
		cell = row.createCell(5);
		cell.setCellValue(new HSSFRichTextString("字段英文名"));
		cell = row.createCell(6);
		cell.setCellValue(new HSSFRichTextString("字段中文名"));
		cell = row.createCell(7);
		cell.setCellValue(new HSSFRichTextString("字段类型"));
		cell = row.createCell(8);
		cell.setCellValue(new HSSFRichTextString("代码取值"));
		cell = row.createCell(9);
		cell.setCellValue(new HSSFRichTextString("代码注释"));
		cell = row.createCell(10);
		cell.setCellValue(new HSSFRichTextString("字段注释"));
		cell = row.createCell(11);
		cell.setCellValue(new HSSFRichTextString("问题/备注"));
	}
	
	/** 
	 * 填入字段落地代码信息
	 * add by chenbo 2012-12-04
	 * @param sheet
	 * @param fields
	 */
	private void createSheet7Row(HSSFSheet sheet, List<ColumnVO> fields){
		try {
			HSSFCell cell = null;
			int num = 1;
			for (ColumnVO field : fields) {
				int startIndex = -1,endIndex=-1;
				//先确定是否有字段落地代码信息，如果存在则填入信息，否则无需填写
				if(field.getFieldName()!=null){
					startIndex = field.getFieldName().indexOf("{{");
					endIndex = field.getFieldName().indexOf("}}");
				}
				if(startIndex>=0 && endIndex>0){
					String commentStr = field.getFieldName().substring(startIndex+2, endIndex);
					/**
					 * 截取字符串时注意中英文符号,如果存在中文符号，先替换成英文符号
					 * 如 ：中文；英文;
					 * 中文：英文:
					 */
					if(commentStr.indexOf("；")>0){
						commentStr = commentStr.replace("；", ";");
					}
					if(commentStr.indexOf("：")>0){
						commentStr = commentStr.replace("：", ":");
					}
					String[] comments = null;
					comments = commentStr.split(";");
				
					for(int i=0;i<comments.length;i++){
						String[] commentBean = null;
						commentBean = comments[i].split(":");
						HSSFRow row1 = sheet.createRow(num);
						//填入系统名称
						cell = row1.createCell(0);
						cell.setCellValue(new HSSFRichTextString(field.getSysName()));
						//系统英文名称
						cell = row1.createCell(1);
						cell.setCellValue(new HSSFRichTextString(field.geteSysName()));
						//系统模块
						cell = row1.createCell(2);
						cell.setCellValue(new HSSFRichTextString(field.getDbName()));
						//表英文名
						cell = row1.createCell(3);
						cell.setCellValue(new HSSFRichTextString(field.geteTableName()));
						//表中文名
						cell = row1.createCell(4);
						cell.setCellValue(new HSSFRichTextString(field.getTableName()));
						//字段英文名
						cell = row1.createCell(5);
						cell.setCellValue(new HSSFRichTextString(field.geteFieldName()));
						//字段中文名
						cell = row1.createCell(6);
						int j = -1;
						if(field.getFieldName()!=null)
							j = field.getFieldName().indexOf("{{");
						if(j>0){
							cell.setCellValue(new HSSFRichTextString(field.getFieldName().substring(0,j)));
						}else{
							cell.setCellValue(new HSSFRichTextString(field.getFieldName()));
						}
						//字段类型
						cell = row1.createCell(7);
						cell.setCellValue(new HSSFRichTextString(field.getFieldType()));
						//代码取值
						cell = row1.createCell(8);
						if(commentBean[0]!=null){
							cell.setCellValue(new HSSFRichTextString(commentBean[0]));
						}else{
							cell.setCellValue(new HSSFRichTextString(""));
						}
						//代码注释
						cell = row1.createCell(9);
						if(commentBean[1]!=null){
							cell.setCellValue(new HSSFRichTextString(commentBean[1]));
						}else{
							cell.setCellValue(new HSSFRichTextString(""));
						}
						//字段注释
						cell = row1.createCell(10);
						if(field.getFieldName()!=null)
							j = field.getFieldName().indexOf("{{");
						if(j>0){
							cell.setCellValue(new HSSFRichTextString(field.getFieldName().substring(j)));
						}else{
							cell.setCellValue(new HSSFRichTextString(""));
						}
						//问题/备注
						cell = row1.createCell(11);
						cell.setCellValue(new HSSFRichTextString(""));
						num++;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * 通过数据库查询的数据创建一个HSSFWorkbook
	 * 
	 * @throws IOException 
	 * @throws FileNotFoundException   
	  * @Title: getHSSFWorkbook  
	  * @Description: TODO 
	  * @param @param sysInfos
	  * @param @param tpl
	  * @param @return 
	  * @return 返回通过实体类生成的HSSFWorkbook
	  * @throws  
	  */
	public Workbook getHSSFWorkbook(SystemVO sysInfos) throws IOException {
		log.info("Start Excel...");
		// 创建一个Excel文件
		HSSFWorkbook workbook = new HSSFWorkbook();
		// 创建Excel的Sheet,总共5个
		// 1封面,2系统级信息，3表级信息，
		// 4字段级信息说明，5字段落地代码
		//封面
		HSSFSheet sheet = workbook.createSheet();
		//系统级信息
		HSSFSheet sheet3 = workbook.createSheet();
		createSheet3Row(sheet3, sysInfos);
		//表级信息
		HSSFSheet sheet4 = workbook.createSheet();
		createSheet4TopRow(sheet4);
		//字段级信息说明
		HSSFSheet sheet5 = workbook.createSheet();
		createSheet5TopRow(sheet5);
		List<SchemaVO> lists = sysInfos.getSchemas();
		for (SchemaVO schemas : lists) {
			createSheet4Row(sheet4, schemas.getTables());
			schemas.getTables().clear();
			createSheet5Row(sheet5, schemas.getFields());
		}
		//字段落地代码
		HSSFSheet sheet7 = workbook.createSheet();
		createSheet7TopRow(sheet7);
		/**
		 * 在字段落地代码sheet中填入落地代码信息
		 * modify by chenbo 2012-12-04
		 */
		for (SchemaVO schemas : lists) {
			createSheet7Row(sheet7, schemas.getFields());
			schemas.getFields().clear();
		}
//		File f = new File("D:\\test.xls");
//		if(f.isFile()){
//			f.delete();
//		}
//		f.createNewFile();
//		FileOutputStream fo = new FileOutputStream(f);
//		workbook.write(fo);
		log.info("End Excel...");
		return workbook;
	}
}
