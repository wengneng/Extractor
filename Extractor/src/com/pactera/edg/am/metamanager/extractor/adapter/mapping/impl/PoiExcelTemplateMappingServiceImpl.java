package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsConfig;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateComp;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateDep;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateMdAttr;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleJoint;
import com.pactera.edg.am.metamanager.core.util.POIExcelUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper.AfterTemplateMappingServiceHelper;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper.DeleteDataBo;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper.AmCommonCodeDeleteDataGenerator;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper.AmCustomDeleteDataGenerator;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper.AmProductDeleteDataGenerator;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper.AmTemplateDeleteDataGenerator;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper.MappingDeleteDataGenerator;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AbstractMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 处理规则：按照元模型树型结构自底向上处理，数据以后进的为准 元数据项目：如果SHEET名称以＂删除＂开头，则该SHEET跳过不处理;
 * 同时将"删除"SHEET的数据，添加至删除列表中，供数据删除使用
 * 
 * @author user
 * @version 1.0 Date: Dec 23, 2010
 * 
 */
public class PoiExcelTemplateMappingServiceImpl extends BaseMappingServiceImpl implements IMetadataMappingService {

	// private final static String SEPARATOR = "-";
	// 需删除的数据的SHEET的名称前缀
	private final static String DELETE_PRIFIX_SHEET = "删除";

	private final static Map<String, String> classifierSheetMap = new HashMap<String, String>(2);
	{
		classifierSheetMap.put("AmCommonRealCodeItem", "删除公共落地代码值");
		classifierSheetMap.put("AmCommonRealCode", "删除公共落地代码表");
		classifierSheetMap.put("AmProduct", "删除产品清单");
		classifierSheetMap.put("ColumnMapping", "删除ETL映射");
	}

	private final static Map<Integer, String> EXCEL_COLUMN_MAPPING = new HashMap<Integer, String>(40);
	{
		EXCEL_COLUMN_MAPPING.put(0, "A");
		EXCEL_COLUMN_MAPPING.put(1, "B");
		EXCEL_COLUMN_MAPPING.put(2, "C");
		EXCEL_COLUMN_MAPPING.put(3, "D");

		EXCEL_COLUMN_MAPPING.put(4, "E");
		EXCEL_COLUMN_MAPPING.put(5, "F");
		EXCEL_COLUMN_MAPPING.put(6, "G");
		EXCEL_COLUMN_MAPPING.put(7, "H");

		EXCEL_COLUMN_MAPPING.put(8, "I");
		EXCEL_COLUMN_MAPPING.put(9, "J");
		EXCEL_COLUMN_MAPPING.put(10, "K");
		EXCEL_COLUMN_MAPPING.put(11, "L");

		EXCEL_COLUMN_MAPPING.put(12, "M");
		EXCEL_COLUMN_MAPPING.put(13, "N");
		EXCEL_COLUMN_MAPPING.put(14, "O");
		EXCEL_COLUMN_MAPPING.put(15, "P");

		EXCEL_COLUMN_MAPPING.put(16, "Q");
		EXCEL_COLUMN_MAPPING.put(17, "R");
		EXCEL_COLUMN_MAPPING.put(18, "S");
		EXCEL_COLUMN_MAPPING.put(19, "T");

		EXCEL_COLUMN_MAPPING.put(20, "U");
		EXCEL_COLUMN_MAPPING.put(21, "V");
		EXCEL_COLUMN_MAPPING.put(22, "W");
		EXCEL_COLUMN_MAPPING.put(23, "X");

		EXCEL_COLUMN_MAPPING.put(24, "Y");
		EXCEL_COLUMN_MAPPING.put(25, "Z");
		EXCEL_COLUMN_MAPPING.put(26, "AA");
		EXCEL_COLUMN_MAPPING.put(27, "AB");

		EXCEL_COLUMN_MAPPING.put(28, "AC");
		EXCEL_COLUMN_MAPPING.put(29, "AD");
		EXCEL_COLUMN_MAPPING.put(30, "AE");
		EXCEL_COLUMN_MAPPING.put(31, "AF");

		EXCEL_COLUMN_MAPPING.put(32, "AG");
		EXCEL_COLUMN_MAPPING.put(33, "AH");
		EXCEL_COLUMN_MAPPING.put(34, "AI");
		EXCEL_COLUMN_MAPPING.put(35, "AJ");

		EXCEL_COLUMN_MAPPING.put(36, "AK");
		EXCEL_COLUMN_MAPPING.put(37, "AL");
		EXCEL_COLUMN_MAPPING.put(38, "AM");
		EXCEL_COLUMN_MAPPING.put(39, "AN");
	}

	private Log log = LogFactory.getLog(PoiExcelTemplateMappingServiceImpl.class);

	public Workbook wBook;

	// 标题开始行，标题结束行，数据开始行,数据结束行
	private int tStart, tEnd, dStart, dEnd;

	// 当前元模型
	private String classifierId;

	// 元模型相对于SHEET的上下文配置
	public MetaModelContext metaModelContext;

	// 元模型间的依赖关系相对于SHEET的上下文配置
	public MetaDependencyContext mdContext;

	// 自底向上，已经处理过的元模型缓存于此
	public Set<TemplateClsConfig> finishedTcConfigs = new HashSet<TemplateClsConfig>();

	// 已经处理过的元模型，及相应的SHEET：元模型，SHEET名称
	// private Map<String, Set<String>> finishedMetaModels = new HashMap<String,
	// Set<String>>();

	// 在处理过程中，顺带处理的祖先结点
	public Map<String, Set<String>> forefatherMetaModels = new HashMap<String, Set<String>>();

	/**
	 * 缓存出现问题的元数据：SHEET名称，行，列
	 */
	public Map<String, Map<Integer, Set<Integer>>> metadataExceptionDataCache = new HashMap<String, Map<Integer, Set<Integer>>>();

	/**
	 * 缓存出现问题的被依赖端元数据：SHEET名称，行，列
	 */
	public Map<String, Map<Integer, Set<Integer>>> valueDependencyExceptionDataCache = new HashMap<String, Map<Integer, Set<Integer>>>();

	/**
	 * 依赖关系的去重 key:依赖关系的hashCode;value:行数
	 * 
	 */
	public Map<String, String> deplicateDependency = null;

	/**
	 * 依赖关系
	 */
	public List<MMDDependency> dependencies;

	public AppendMetadata aMetadata;

	// 需转换成大写的变量;需转换成小写的变量
	private boolean isUpperCase = false, isLowerCase = false;

	/**
	 * 该元模型的全路径在模板中的位置(包括自身)
	 */
	public List<DeleteDataBo> deleteDataBos = new ArrayList<DeleteDataBo>();

	public PoiExcelTemplateMappingServiceImpl()
	{
		
		// 默认不区分大小写改为区分大小写 wangjian
		isUpperCase = true;
	}

	private List<TemplateTitle> title = new ArrayList<TemplateTitle>();
	
	public void metadataMapping(AppendMetadata aMetadata) throws Exception {
		this.aMetadata = aMetadata;

		// 获取模板编号，取得模板配置
		IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance().getIClassifier();
		String dsId = AdapterExtractorContext.getInstance().getDatasourceId();
		List<TemplateClsConfig> tcConfigs = iClassifier.getTemplateClsConfigsByDs(dsId);

		// 获取模板编号，取得模板配置结束
		String path = AdapterExtractorContext.getInstance().getDsAbsolutePath();
		// path =
		// "E:\\work\\MetaManager元数据管理产品\\元数据银行\\EDW\\数据标准\\standard\\客户信息模板.xls";

		try {

			log.info(new StringBuilder("开始解析Excel文件,文件绝对路径:").append(path).append(" ,...").toString());
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, new StringBuilder("开始解析Excel文件,文件绝对路径:")
					.append(path).append(" ,...").toString());
			//对于华润银行需要对Excel内容进行大写转换
//			wBook = JiCaiExcelUtil.init().ExeclValueToUpperCase(path);

			wBook = new HSSFWorkbook(new FileInputStream(new File(path)));
			
			iterTemplateConf(tcConfigs);
			
			setDependencies(tcConfigs);

			aMetadata.setChildMetaModels(aMetadata.getMetaModel().getChildMetaModels());
			aMetadata.setDDependencies(dependencies);

			afterExtracted();

			printExceptionData();
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
			wBook = null;
			this.aMetadata = null;
			dependencies = null;
			metaModelContext = null;

			mdContext = null;
			finishedTcConfigs.clear();
			// finishedMetaModels.clear();
			forefatherMetaModels.clear();

			metadataExceptionDataCache.clear();
			valueDependencyExceptionDataCache.clear();
			deleteDataBos.clear();
			deplicateDependency = null;
			dependencies = null;
		}
	}

	public void afterExtracted() {
		AfterTemplateMappingServiceHelper helper = new AfterTemplateMappingServiceHelper(aMetadata);
		helper.afterExtracted();

		if (deleteDataBos.size() > 0) {
			String sheetName = deleteDataBos.get(0).getSheetName();
			Map<String, List<Map<String, String>>> deleteDataMap = new HashMap<String, List<Map<String, String>>>();
			// 获取需要删除的数据
			if(sheetName.indexOf("产品")!=-1){
				AmProductDeleteDataGenerator deleteDataGenerator;
				deleteDataGenerator = new AmProductDeleteDataGenerator(wBook, deleteDataBos,
						isUpperCase, isLowerCase);
				deleteDataMap = deleteDataGenerator.generateDeleteData();
			}else if(sheetName.indexOf("客户")!=-1){
				AmCustomDeleteDataGenerator deleteDataGenerator;
				deleteDataGenerator = new AmCustomDeleteDataGenerator(wBook, deleteDataBos,isUpperCase, isLowerCase);
				deleteDataMap = deleteDataGenerator.generateDeleteData();
			}else if(sheetName.indexOf("公共代码")!=-1){
				AmCommonCodeDeleteDataGenerator deleteDataGenerator;
				deleteDataGenerator = new AmCommonCodeDeleteDataGenerator(wBook, deleteDataBos,isUpperCase, isLowerCase);
				deleteDataMap = deleteDataGenerator.generateDeleteData();
			}else if(sheetName.indexOf("ETL映射")!=-1){
				MappingDeleteDataGenerator deleteDataGenerator;
				deleteDataGenerator = new MappingDeleteDataGenerator(wBook, deleteDataBos,isUpperCase, isLowerCase);
				deleteDataMap = deleteDataGenerator.generateDeleteData();
			}else{//数据字典	
				AmTemplateDeleteDataGenerator deleteDataGenerator = new AmTemplateDeleteDataGenerator(wBook, deleteDataBos,
						isUpperCase, isLowerCase);
				deleteDataMap = deleteDataGenerator.generateDeleteData();
			}
			aMetadata.setDeleteMapDatas(deleteDataMap);
		}
	}

	public void setDependencies(List<TemplateClsConfig> tcConfigs) {
		mdContext = new MetaDependencyContext();
		dependencies = new ArrayList<MMDDependency>();
		deplicateDependency = new HashMap<String, String>();

		iterSetDependencies(tcConfigs);

	}

	private void iterSetDependencies(List<TemplateClsConfig> tcConfigs) {
		for (TemplateClsConfig tcConfig : tcConfigs) {
			// 元模型列表
			iterSetDependencies(tcConfig);

			// 组合关系，递归向下
			iterSetDependencies(tcConfig.getCompeds());
		}
	}

	private void iterSetDependencies(TemplateClsConfig tcConfig) {
		List<TemplateClsTitle> tcTitles = tcConfig.getClsTitles();

		for (TemplateClsTitle tcTitle : tcTitles) {
			// 元模型的配置列表
			// 1.先判断该配置有没有配置依赖关系
			List<TemplateDep> deps = tcTitle.getDeps();
			if (deps.size() == 0) {
				continue;
			}
			// 数据结束标记
			Integer tmpDEnd = tcTitle.getDend();
			if (tmpDEnd != null) {
				dEnd = tmpDEnd.intValue();
			}
			// 数据开始行
			dStart = tcTitle.getDstart();
			// 标题开始行
			tStart = tcTitle.getTstart();
			// 标题结束行
			tEnd = tcTitle.getTend();

			// 2.有配置依赖关系，则先获取OWNER端的元数据信息
			int sheetSize = wBook.getNumberOfSheets();
			// SHEET从0-SIZE-1
			for (int sheetIndex = 0; sheetIndex < sheetSize; sheetIndex++) {
				// 元模型的配置中的祖先结点的配置
				Sheet sheet = wBook.getSheetAt(sheetIndex);

				if (isDeleteSheet(sheet))
					continue;
				// 校验依赖关系所需的配置在当前SHEET中是否配置正确,并建立依赖关系
				setDependencySheet(tcConfig.getClsid(), tcTitle, sheet);
			}
		}

	}

	private void printDependencyInfo(String sheetName) {
		StringBuilder sb = new StringBuilder();
		sb.append("SHEET:").append(sheetName).append(" 符合条件获取元模型间的依赖关系,配置如下:\n\t依赖端路径配置：");
		for (Iterator<String> ownerIter = mdContext.ownerPositionMapping.keySet().iterator(); ownerIter.hasNext();) {
			String key = ownerIter.next();
			sb.append(key).append("==>").append(getColumnsPosition(mdContext.ownerPositionMapping.get(key)))
					.append(";");
		}
		sb.append("\n\t被依赖端路径配置:");
		for (Iterator<String> valueIter = mdContext.valuePositionMapping.keySet().iterator(); valueIter.hasNext();) {
			String key = valueIter.next();
			sb.append(key).append("==>").append(getColumnsPosition(mdContext.valuePositionMapping.get(key)))
					.append(";");
		}
		log.info(sb.toString());
		AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, sb.toString());
	}

	private String getColumnsPosition(int[] positions) {
		StringBuilder sb = new StringBuilder("[");
		for (int index = 0; index < positions.length; index++) {
			if (EXCEL_COLUMN_MAPPING.containsKey(positions[index])) {
				sb.append(EXCEL_COLUMN_MAPPING.get(positions[index])).append(" ,");
			}
			else {
				sb.append(positions[index]).append(" ,");
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("]");
		return sb.toString();
	}

	private String getColumnsPosition(Set<Integer> positions) {
		StringBuilder sb = new StringBuilder("[");
		for (int position : positions) {
			if (EXCEL_COLUMN_MAPPING.containsKey(position)) {
				sb.append(EXCEL_COLUMN_MAPPING.get(position)).append(" ,");
			}
			else {
				sb.append(position).append(" ,");
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("]");
		return sb.toString();
	}

	private void setDependencyFromSheet(Sheet sheet) {
		int rowNum = sheet.getLastRowNum();
		for (int rowIndex = 0; rowIndex <= rowNum; rowIndex++) {
			try {
				Row row = sheet.getRow(rowIndex);
				if (row == null)
					continue;

				// 找寻owner元数据
				MMMetadata ownerMetadata = findDependencyMetadata(mdContext.ownerPositionMapping, row);
				if (ownerMetadata == null) {
					// owner元数据没有找着
					continue;
				}
				// 找寻value元数据
				MMMetadata valueMetadata = findDependencyMetadata(mdContext.valuePositionMapping, row);
				if (valueMetadata == null) {
					// value元数据没有找着
					cacheExceptionData(row);

					continue;
				}
				setDependency(ownerMetadata, valueMetadata, rowIndex, sheet.getSheetName());
			}
			catch (Exception e) {
				String sb = new StringBuilder("SHEET:").append(sheet.getSheetName()).append(",在依赖关系处理第").append(
						rowIndex + 1).append("行时，出现异常!").toString();
				log.error(sb, e);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, sb);
			}
		}

	}

	private void cacheExceptionData(Row row) {
		String key = null;
		for (Iterator<String> keyIter = mdContext.valuePositionMapping.keySet().iterator(); keyIter.hasNext();) {
			key = keyIter.next();
		}
		if (key == null)
			return;
		int[] position = mdContext.valuePositionMapping.get(key);
		String value = POIExcelUtil.getCellValue(position, row);
		if (value.equals("")) // 被依赖端没有填写数据，则不缓存之
			return;

		cacheExceptionData(valueDependencyExceptionDataCache, position, row);
	}

	private void setDependency(MMMetadata ownerMetadata, MMMetadata valueMetadata, int rowIndex, String sheetName) {
		MMDDependency dependency = new MMDDependency();
		dependency.setOwnerMetadata(ownerMetadata);
		dependency.setValueMetadata(valueMetadata);
		if (mdContext.ownerRole != null && !mdContext.ownerRole.equals("")) {
			dependency.setOwnerRole(mdContext.ownerRole);
		}
		if (mdContext.valueRole != null && !mdContext.valueRole.equals("")) {
			dependency.setValueRole(mdContext.valueRole);
		}
		if (!deplicateDependency.containsKey(dependency.getUid())) {
			// 依赖关系没有重复
			deplicateDependency.put(dependency.getUid(), "SHEET:".concat(sheetName).concat(",第") + (rowIndex + 1));
			dependencies.add(dependency);
		}
		else {
			// 依赖关系重复了
			StringBuilder sb = new StringBuilder();

			sb.append("依赖关系数据重复[").append(ownerMetadata.getClassifierId()).append("　与　").append(
					valueMetadata.getClassifierId()).append(" 间的依赖]：SHEET:").append(sheetName).append(",第").append(
					rowIndex + 1).append("行与").append(deplicateDependency.get(dependency.getUid())).append("行重复!");
			log.warn(sb.toString());
			// AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.WARN,
			// sb.toString());
		}
	}

	/**
	 * 根据缓存信息，查找依赖关系一端的元数据信息，如不存在则返回null
	 * 
	 * @param cacheMetadatas
	 * @param row
	 * @return
	 */
	private MMMetadata findDependencyMetadata(Map<String, int[]> cacheMetadatas, Row row) {
		MMMetadata metadata = aMetadata.getMetadata();
		for (Iterator<String> iter = cacheMetadatas.keySet().iterator(); iter.hasNext();) {
			String classifierId = iter.next();
			int[] positions = cacheMetadatas.get(classifierId);
			String code = getCode(POIExcelUtil.getCellValue(positions, row));
			if (code.equals("")) {
				// 如果为空
				// cacheExceptionData(positions, row);
				return null;
			}
			boolean metadataIsExist = false;
			List<AbstractMetadata> children = metadata.getChildrenMetadatas();
			for (AbstractMetadata childMetadata : children) {
				if (childMetadata.getCode().equals(code) && childMetadata.getClassifierId().equals(classifierId)) {
					// 找到元数据
					metadataIsExist = true;
					metadata = (MMMetadata) childMetadata;
					break;
				}
			}
			if (!metadataIsExist) { return null; }

		}
		return metadata;
	}

	/**
	 * 校验依赖关系是否可从该SHEET中获取
	 * 
	 * @param classifierId
	 * @param tcTitle
	 * @param sheet
	 * @return
	 */
	private void setDependencySheet(String classifierId, TemplateClsTitle tcTitle, Sheet sheet) {

		List<TemplateComp> forefather = tcTitle.getComps();
		boolean dependencyConfIsRight = true;

		for (TemplateComp ownerComp : forefather) {
			dependencyConfIsRight = iterDependencyTemplateTitleJoint(MetaDependencyContext.OWNER_FLAG, ownerComp
					.getCompClsId(), ownerComp.getTitleJnts().getTitleJoints(), sheet);
			if (!dependencyConfIsRight) {
				mdContext.ownerPositionMapping.clear();
				return;
			}
		}
		dependencyConfIsRight = iterDependencyTemplateTitleJoint(MetaDependencyContext.OWNER_FLAG, classifierId,
				tcTitle.getMdCode().getTitleJoints(), sheet);
		if (!dependencyConfIsRight) {
			mdContext.ownerPositionMapping.clear();
			return;
		}
		List<TemplateDep> deps = tcTitle.getDeps();
		for (TemplateDep dep : deps) {
			// 元模型配置的依赖关系列表
			mdContext.ownerRole = dep.getFrole();
			mdContext.valueRole = dep.getTrole();
			for (TemplateComp valueComp : dep.getRelateConds()) {
				dependencyConfIsRight = iterDependencyTemplateTitleJoint(MetaDependencyContext.VALUE_FLAG, valueComp
						.getCompClsId(), valueComp.getTitleJnts().getTitleJoints(), sheet);
			}
			if (dependencyConfIsRight) {
				// 成功找到被依赖端的配置
				printDependencyInfo(sheet.getSheetName());
				setDependencyFromSheet(sheet);
			}

			mdContext.valuePositionMapping.clear();
			mdContext.ownerRole = null;
			mdContext.valueRole = null;
		}
		mdContext.ownerPositionMapping.clear();
	}

	/**
	 * 缓存依赖关系的相关配置
	 * 
	 * @param type
	 * @param key
	 * @param joints
	 * @param sheet
	 * @return
	 */
	private boolean iterDependencyTemplateTitleJoint(byte type, String key, List<TemplateTitleJoint> joints, Sheet sheet) {
		int jointSize = joints.size();
		if (jointSize == 0)
			return false;
		int[] position = initPosition(jointSize);

		for (int i = 0; i < jointSize; i++) { // 关键字可由多列共同组成
			TemplateTitleJoint tj = joints.get(i);
			List<TemplateTitle> titles = tj.getTitles();
			// 横向合并，为0为没有合并；大于0则为合并的单元格个数
			int mergeAcrs = -1;
			short columnIndex = -1;
			for (int j = 0; j < titles.size(); j++) { // 标题可能由一至多行组成，多行需遍历判断每一行
				TemplateTitle title = titles.get(j);
				// CODE对应在SHEET中的列
				String titleName = title.getName();
				mergeAcrs = title.getMergeAcrs();
				if (mergeAcrs > 0) {
					columnIndex = getColumnIndex(sheet, titleName, tStart - 1 + j);
					break;
				}
			}

			// 标题行,注意：ROW为从0开始计，故此处为tEnd-1
			Row titleRow = sheet.getRow(tEnd - 1);
			if (titleRow == null)
				return false;

			String endKey = titles.get(titles.size() - 1).getName();
			int cntPosition = getColumnIndex(mergeAcrs, columnIndex, titleRow, endKey);
			if (cntPosition == -1) {
				return false;
			}
			else {
				position[i] = cntPosition;
			}
		}
		if (type == MetaDependencyContext.OWNER_FLAG) {
			mdContext.ownerPositionMapping.put(key, position);

		}
		else
			mdContext.valuePositionMapping.put(key, position);
		return true;
	}

	public void printExceptionData() {
		StringBuilder sb = new StringBuilder();
		if (metadataExceptionDataCache.size() > 0) {
			sb.append("如下数据可能存在错误:\n\tSHEET:");
			for (Iterator<String> keyIter = metadataExceptionDataCache.keySet().iterator(); keyIter.hasNext();) {
				String sheetName = keyIter.next();
				sb.append(sheetName);
				Map<Integer, Set<Integer>> value = metadataExceptionDataCache.get(sheetName);
				for (Iterator<Integer> rowIndexIter = value.keySet().iterator(); rowIndexIter.hasNext();) {
					int rowIndex = rowIndexIter.next();
					sb.append(", 第").append(rowIndex).append("行, 第").append(getColumnsPosition(value.get(rowIndex)))
							.append("列\n\t");
				}
			}
		}
		if (valueDependencyExceptionDataCache.size() > 0) {
			sb.append("如下关联数据可能存在错误[关联的对象不存在]:\n\tSHEET:");
			for (Iterator<String> keyIter = valueDependencyExceptionDataCache.keySet().iterator(); keyIter.hasNext();) {
				String sheetName = keyIter.next();
				sb.append(sheetName);
				Map<Integer, Set<Integer>> value = valueDependencyExceptionDataCache.get(sheetName);
				for (Iterator<Integer> rowIndexIter = value.keySet().iterator(); rowIndexIter.hasNext();) {
					int rowIndex = rowIndexIter.next();
					sb.append(", 第").append(rowIndex).append("行, 第").append(getColumnsPosition(value.get(rowIndex)))
							.append("列\n\t");
				}
			}
		}

		if (sb != null) {
			addExtractorLog(ExtractorLogLevel.WARN, sb);

		}

	}

	private void addExtractorLog(ExtractorLogLevel level, StringBuilder sb) {
		int count = sb.length() / AdapterExtractorContext.getInstance().getMaxMetadataAttrSize() + 1;
		int start = 0, end = 0;
		for (int index = 0; index < count; index++) {
			start = end;
			if (index == count - 1) {
				end = sb.length();
				if (start == end)
					break;
			}
			else {
				end += AdapterExtractorContext.getInstance().getMaxMetadataAttrSize();
			}
			if (level == ExtractorLogLevel.WARN) {
				log.warn(sb.substring(start, end).toString());
			}
			else {
				log.info(sb.substring(start, end).toString());
			}
			AdapterExtractorContext.addExtractorLog(level, sb.substring(start, end).toString());
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
		this.classifierId = tcConfig.getClsid();
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

	/**
	 * 根据元模型的配置，遍历SHEET，并判断之，如符合条件则从中获取并创建元数据
	 * 
	 * @param tcTitle
	 */
	private void iterTemplateClsTitle(TemplateClsTitle tcTitle) {
		// 数据结束标记
		Integer tmpDEnd = tcTitle.getDend();
		if (tmpDEnd != null) {
			dEnd = tmpDEnd.intValue();
		}
		// 数据开始行
		dStart = tcTitle.getDstart();
		// 标题开始行
		tStart = tcTitle.getTstart();
		// 标题结束行
		tEnd = tcTitle.getTend();

		// ------------------------
		// 查询符合条件的SHEET,找到一个处理一个
		int sheetSize = wBook.getNumberOfSheets();
		// SHEET从0-SIZE-1
		metaModelContext = new MetaModelContext();
		for (int sheetIndex = 0; sheetIndex < sheetSize; sheetIndex++) {
			// 对于每一个元模型,遍历在每一个SHEET中校验，从符合规格的SHEET中获取元模型的数据
			// 遍历SHEET
			Sheet sheet = wBook.getSheetAt(sheetIndex);

			// 判断是符合从当前SHEET获取当前元模型的数据
			boolean sheetIsRight = validateSheet(tcTitle, sheet);
			if (sheetIsRight) {
//				if (forefatherMetaModels.containsKey(classifierId)
//						&& forefatherMetaModels.get(classifierId).contains(sheet.getSheetName())
//						&& metaModelContext.metaPositionMapping.size() == 2) {
					// 该结点已经简略的处理过，即只获取了其code,name，如果本次处理也只是取这两个元素，则不必要了
					// 该元模型已经有处理，且在当前SHEET有处理，且本次获取的元数据不当前SHEET不止只有两个元素，则认为已经处理过的
					// 可能存在问题：相同的元模型在同一SHEET配置了两次以上，且某一次的元数据只配置了两个信息（指代码，名称），则此时也会认为
					// 是已经处理过的,待完善!!!
//				}
//				else {
					// 符合从当前SHEET获取信息
					genMetadatasFromSheet(sheet);
					// 把处理过的SHEET缓存起来
					cacheFinishedMetaModels(sheet.getSheetName());
//				}
				// 该配置只针对当前SHEET有效，当处理下一个SHEET时，需要全新的缓存了，故新建一个
				metaModelContext = new MetaModelContext();
			}
			else {
				// 当前SHEET没有找到符合当前元模型的配置
			}

		}
	}

	private void cacheFinishedMetaModels(String sheetName) {
		// 缓存当前元模型在当前SHEET的痕迹
		// cacheFinishedMetaModels(classifierId, sheetName);
		// 缓存祖先元模型在当前SHEET的痕迹
		for (Iterator<String> parentIter = metaModelContext.parentPositionMapping.keySet().iterator(); parentIter
				.hasNext();) {
			cacheForefatherMetaModels(parentIter.next(), sheetName);
		}

	}

	private void cacheForefatherMetaModels(String next, String sheetName) {
		if (forefatherMetaModels.containsKey(classifierId)) {
			forefatherMetaModels.get(classifierId).add(sheetName);
		}
		else {
			Set<String> sheetSet = new HashSet<String>();
			sheetSet.add(sheetName);
			forefatherMetaModels.put(classifierId, sheetSet);
		}
	}

	/**
	 * 从合法的SHEET中获取并创建元数据信息
	 * 
	 * @param sheet
	 */
	private void genMetadatasFromSheet(Sheet sheet) {
		// 结束行有设置,且结束行大于等于开始行,则最大行数采用结束行,否则采用SHEET的最大行
		int maxRowNum = (dEnd >= dStart) ? dEnd - 1 : sheet.getLastRowNum();

		for (int rowIndex = dStart - 1; rowIndex <= maxRowNum; rowIndex++) {
			try {
				Row row = sheet.getRow(rowIndex);

				if (row == null)
					continue;

				boolean rowIsBlank = rowIsBlank(row);
				if (rowIsBlank) // 该行虽然不为NULL,但每一个CELL均为NULL或''，故也不处理之
					continue;

				MMMetaModel parentMetaModel = aMetadata.getMetaModel();
				MMMetadata parentMetadata = aMetadata.getMetadata();
				boolean dataIsLegal = true;
				for (Iterator<String> keyIter = metaModelContext.parentPositionMapping.keySet().iterator(); keyIter
						.hasNext();) {
					// 祖先结点的元模型
					String classifierId = keyIter.next();
					// 祖先结点的元模型在SHEET中的位置
					int[] position = metaModelContext.parentPositionMapping.get(classifierId);
					String code = getCode(POIExcelUtil.getCellValue(position, row));
					if (code.equals("")) {
						// 某一级的父结点为空
						cacheExceptionData(metadataExceptionDataCache, position, row);
						dataIsLegal = false;
						break;
					}
				

					// 当前元模型
					MMMetaModel metaModel = parentMetaModel.getChildMetaModel(classifierId);
					if (metaModel == null) {
						// 还没有创建这个元模型及元数据
						parentMetaModel.addChildMetaModel(classifierId);
						metaModel = parentMetaModel.getChildMetaModel(classifierId);
						// metaModel.setHaveChangelessAttr(true);
					}

					boolean metadataIsExist = false;
					List<AbstractMetadata> metadatas = parentMetadata.getChildrenMetadatas();
					if(row.getCell(8)!=null){
						if(this.classifierId.equals("AmTable")&&POIExcelUtil.getCellValue(row.getCell(8)).length()<1)
						{		
							row.getCell(8).setCellValue("重要");
						}
					}
					for (AbstractMetadata metadata : metadatas) {
						if (metadata.getCode().equals(code) && metadata.getClassifierId().equals(classifierId)) {
							// 找到父结点,则不替换
							metadataIsExist = true;
							parentMetadata = (MMMetadata) metadata;
							break;
						}
					}
					if (!metadataIsExist) {
						// 该元数据不存在于列表中，则新创建一个
						MMMetadata cntMetadata = super.createMetadata(parentMetadata, metaModel, code, null);

						parentMetadata = cntMetadata;
					}
					parentMetaModel = metaModel;

				}

				if (dataIsLegal) {
					// 合法的数据，才取当前结点
					createNewMetadata(row, parentMetaModel, parentMetadata);

				}
			}
			catch (Exception e) {
				String sb = new StringBuilder("SHEET:").append(sheet.getSheetName()).append(",元模型：").append(
						classifierId).append(",在处理第").append(rowIndex + 1).append("行时，出现异常!").toString();
				log.error(sb, e);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR, sb);
			}

		}
	}

	private String getCode(String cellValue) {
		if (isUpperCase) { return cellValue.toUpperCase(); }
		if (isLowerCase) { return cellValue.toLowerCase(); }
		return cellValue;
	}

	private boolean rowIsBlank(Row row) {
		for (Iterator<Cell> cells = row.iterator(); cells.hasNext();) {
			Cell cell = cells.next();
			if (cell == null)
				continue;
			if (!POIExcelUtil.getCellValue(cell).equals(""))
				return false;

		}
		return true;
	}

	/**
	 * 创建新的元数据：从row中取到元数据，如果必须信息为空，则退出；如果当前元数据的元模型还没有创建，则创建之；
	 * 如果当前元数据已经存在，则将属性等替换旧的数据
	 * 
	 * @param row
	 * @param parentMetaModel
	 *            父元模型
	 * @param parentMetadata
	 *            父元数据
	 */
	private void createNewMetadata(Row row, MMMetaModel parentMetaModel, MMMetadata parentMetadata) {
		MMMetadata cntMetadata = new MMMetadata();
		setMetadataBaseInfo(cntMetadata, row);
		if (cntMetadata.getCode() == null) {
			// 元数据代码不能为空
			return;
		}

		boolean metadataIsExist = false;
		List<AbstractMetadata> metadatas = parentMetadata.getChildrenMetadatas();
		for (AbstractMetadata metadata : metadatas) {
			if (metadata.getCode().equals(cntMetadata.getCode()) && metadata.getClassifierId().equals(classifierId)) {
				// 找到元数据
				metadataIsExist = true;
				// 原先已录入的数据,信息量可能不全,名称也可能为空
				MMMetadata m = (MMMetadata) metadata;
				if (m.getName() == null || m.getName().equals("")) {
					m.setName(cntMetadata.getName());
				}
				Map<String, String> mAttrs = m.getAttrs();
				Map<String, String> cntMAttrs = cntMetadata.getAttrs();
				for (Iterator<String> iter = cntMAttrs.keySet().iterator(); iter.hasNext();) {
					String key = iter.next();
					if (!mAttrs.containsKey(key)) {
						mAttrs.put(key, cntMAttrs.get(key));
					}
				}
				break;
			}
		}

		if (!metadataIsExist) {
			cntMetadata.setClassifierId(classifierId);
			cntMetadata.setParentMetadata(parentMetadata);
			MMMetaModel metaModel = parentMetaModel.getChildMetaModel(classifierId);
			if (metaModel == null) {
				// 还没有创建这个元模型及元数据
				parentMetaModel.addChildMetaModel(classifierId);
				metaModel = parentMetaModel.getChildMetaModel(classifierId);
				// metaModel.setHaveChangelessAttr(true);
			}

			// 数据还不存在，则添加之
			metaModel.addMetadata(cntMetadata);
			parentMetadata.addChildMetadata(cntMetadata);
			metaModel.setHasMetadata(true);
		}
	}

	/**
	 * 设置元数据的基本信息
	 * 
	 * @param metadata
	 * @param row
	 */
	private void setMetadataBaseInfo(MMMetadata metadata, Row row) {
		for (Iterator<String> keyIter = metaModelContext.metaPositionMapping.keySet().iterator(); keyIter.hasNext();) {
			String key = keyIter.next();
			int[] position = metaModelContext.metaPositionMapping.get(key);
			String value = POIExcelUtil.getCellValue(position, row);
			if (MetaModelContext.CODE_FLAG.equals(key)) {
				// value为CODE
				if (value.equals("")) {
					// 元数据代码不能为空！
					// cacheExceptionData(position, row);
					break;
				}
				metadata.setCode(getCode(value));
			}
			else if (MetaModelContext.NAME_FLAG.equals(key)) {
				// value为NAME
				if (!value.equals(""))
					metadata.setName(getCode(value));
			}
			else {
				// value为属性
				if (!value.equals("")) {
					// 为换行作转换,转换为界面上能够识别的标识<br/>
					metadata.addAttr(key, value.replaceAll("\n", "<br/>"));
				}
			}
		}
	}

	private void cacheExceptionData(Map<String, Map<Integer, Set<Integer>>> cache, int[] position, Row row) {
		String sheetName = row.getSheet().getSheetName();
		int rowNum = row.getRowNum() + 1;

		if (cache.containsKey(sheetName)) {
			if (cache.get(sheetName).containsKey(rowNum)) {
				for (int i = 0; i < position.length; i++) {
					cache.get(sheetName).get(rowNum).add(position[i]);
				}
			}
			else {
				Set<Integer> columnError = new HashSet<Integer>(position.length);
				for (int i = 0; i < position.length; i++) {
					columnError.add(position[i]);
				}
				cache.get(sheetName).put(rowNum, columnError);
			}
		}
		else {
			Map<Integer, Set<Integer>> error = new LinkedHashMap<Integer, Set<Integer>>(1);
			Set<Integer> columnError = new HashSet<Integer>(position.length);
			for (int i = 0; i < position.length; i++) {
				columnError.add(position[i]);
			}
			error.put(rowNum, columnError);
			cache.put(sheetName, error);
		}
	}

	/**
	 * 判断是否当前SHEET获取当前元模型的数据:当前元模型代码,名称+其祖先的元模型+元模型属性
	 * 以上元素都安全的在当前SHEET的头位置，则认为可以从当前SHEET获取当前元模型的数据
	 * 
	 * @param tCls
	 * @param sheet
	 * @return true or false
	 */
	private boolean validateSheet(TemplateClsTitle tCls, Sheet sheet) {

		boolean sheetIsRight = true;
		
		// 元数据代码
		List<TemplateTitleJoint> codeJoints = tCls.getMdCode().getTitleJoints();
		// 1.判断元数据代码是否在当前SHEET中配置
		sheetIsRight = iterTemplateTitleJoint(NodeType.CODE, MetaModelContext.CODE_FLAG, codeJoints, sheet);

		if (!sheetIsRight)// CODE不在当前SHEET中，认为配置错误
			return sheetIsRight;
		// 被组合关系(父结点)
		// 2.判断元数据的祖先结点是否在当前SHEET中配置
		List<TemplateComp> forefathers = tCls.getComps();
		for (TemplateComp forefather : forefathers) {
			List<TemplateTitleJoint> forefatherJoints = forefather.getTitleJnts().getTitleJoints();
			sheetIsRight = iterTemplateTitleJoint(NodeType.FOREPARENT, forefather.getCompClsId(), forefatherJoints,
					sheet);;
			if (!sheetIsRight) {// 某个祖先结点的CODE不在当前SHEET中，认为配置错误
				metaModelContext.parentPositionMapping.clear();
				return sheetIsRight;
			}
		}

		// 判断该SHEET是否以＂删除＂开头
		sheetIsRight = !isDeleteSheet(sheet);
		if (!sheetIsRight) {
			if (!classifierSheetMap.containsKey(classifierId)
					|| (classifierSheetMap.containsKey(classifierId) && classifierSheetMap.get(classifierId).equals(
							sheet.getSheetName()))) {
				DeleteDataBo dBo = new DeleteDataBo();
				dBo.setClassifierId(classifierId);
				dBo.setDStart(dStart);
				dBo.setDEnd(dEnd);
				dBo.setSheetName(sheet.getSheetName());
				Map<String, int[]> positionMapping = new LinkedHashMap<String, int[]>(
						metaModelContext.parentPositionMapping.size() + 1);
				for (Iterator<String> keyIter = metaModelContext.parentPositionMapping.keySet().iterator(); keyIter
						.hasNext();) {
					String key = keyIter.next();
					positionMapping.put(key, metaModelContext.parentPositionMapping.get(key));
				}
				positionMapping.put(classifierId, metaModelContext.metaPositionMapping.get(MetaModelContext.CODE_FLAG));
				dBo.setPathPosition(positionMapping);

				deleteDataBos.add(dBo);
			}
			return sheetIsRight;

		}

		// 元数据名称
		if (tCls.getMdName() != null) {
			// 3.判断元数据的名称是否在当前SHEET中配置
			List<TemplateTitleJoint> nameJoints = tCls.getMdName().getTitleJoints();
			sheetIsRight = iterTemplateTitleJoint(NodeType.NAME, MetaModelContext.NAME_FLAG, nameJoints, sheet);
			if (!sheetIsRight) {// 名称不在当前SHEET中，认为配置错误
				metaModelContext.parentPositionMapping.clear();
				metaModelContext.metaPositionMapping.clear();
				return sheetIsRight;
			}
		}

		// 4.判断元数据属性是否在当前SHEET中配置
		List<TemplateMdAttr> mdAttrs = tCls.getMdAttrs();
		for (TemplateMdAttr mdAttr : mdAttrs) {
			List<TemplateTitleJoint> mdAttrJoints = mdAttr.getTitleJnts().getTitleJoints();
			sheetIsRight = iterTemplateTitleJoint(NodeType.ATTRIBUTE, mdAttr.getAttCode(), mdAttrJoints, sheet);
			if (!sheetIsRight) {// 属性不在当前SHEET中，认为配置错误
				// 元数据主路径配置成功,但某个属性找不到映射,有必要提供日志信息
				StringBuilder sb = new StringBuilder();
				sb.append("数据文件与模板文件格式不一致!模板映射中配置的列名为:").append(mdAttrJoints.get(0).getTitles().get(0).getName())
						.append("[").append(mdAttr.getAttCode()).append("],在当前SHEET:[").append(sheet.getSheetName())
						.append("]中找不到!相应的元模型为:").append(classifierId).append("(其主路径的关键字已经找到).");
				log.warn(sb.toString());
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.WARN, sb.toString());

				metaModelContext.parentPositionMapping.clear();
				metaModelContext.metaPositionMapping.clear();
				return sheetIsRight;
			}
		}
		printMetaModelSheetInfo(sheet);
		// 通过以上四步，认为可以从当前SHEET中获取该元数据的基本信息
		// 注：元数据的属性还没有判断，原因在于属性为非关键信息，如果
		// 某个属性配置错误，则不获取之即可，但不影响对其它配置正确的属性的获取
		return sheetIsRight;
	}

	private boolean isDeleteSheet(Sheet sheet) {
		return sheet.getSheetName().startsWith(DELETE_PRIFIX_SHEET);
	}

	private void printMetaModelSheetInfo(Sheet sheet) {
		StringBuilder sb = new StringBuilder();
		sb.append("SHEET:").append(sheet.getSheetName()).append(" 符合条件获取元模型:").append(classifierId).append(
				"的元数据,详细配置如下:\n\t");
		for (Iterator<String> iter = metaModelContext.metaPositionMapping.keySet().iterator(); iter.hasNext();) {
			String key = iter.next();
			sb.append(key).append("==>").append(getColumnsPosition(metaModelContext.metaPositionMapping.get(key)))
					.append(";");
		}
		if (metaModelContext.parentPositionMapping.size() > 0) {
			sb.append("\n\t祖先元模型的配置:\n\t");
			for (Iterator<String> iter = metaModelContext.parentPositionMapping.keySet().iterator(); iter.hasNext();) {
				String key = iter.next();
				sb.append(key).append("==>")
						.append(getColumnsPosition(metaModelContext.parentPositionMapping.get(key))).append(";");
			}
		}
		addExtractorLog(ExtractorLogLevel.INFO, sb);
	}

	/**
	 * 判断nodeKey是否在当前SHEET中有配置，如有则缓存之，否则返回false
	 * 
	 * @param type
	 * @param nodeKey
	 * @param joints
	 * @param sheet
	 * @return
	 */
	private boolean iterTemplateTitleJoint(NodeType type, String nodeKey, List<TemplateTitleJoint> joints, Sheet sheet) {
		int jointSize = joints.size();
		if (jointSize == 0)
			return false;
		int[] position = initPosition(jointSize);

		for (int i = 0; i < jointSize; i++) { // 关键字可由多列共同组成
			TemplateTitleJoint tj = joints.get(i);
			List<TemplateTitle> titles = tj.getTitles();
			// 横向合并，为0为没有合并；大于0则为合并的单元格个数
			int mergeAcrs = -1;
			short columnIndex = -1;
			for (int j = 0; j < titles.size(); j++) { // 标题可能由一至多行组成，多行需遍历判断每一行
				TemplateTitle title = titles.get(j);
				// CODE对应在SHEET中的列
				String titleName = title.getName();
				mergeAcrs = title.getMergeAcrs();
				if (mergeAcrs > 0) {
					columnIndex = getColumnIndex(sheet, titleName, tStart - 1 + j);
					break;
				}
				// log.info("getMergeAcrs:" + mergeAcrs);
				// log.info("getMergeDown:" + title.getMergeDown());
			}

			// 标题行,注意：ROW为从0开始计，故此处为tEnd-1
			Row titleRow = sheet.getRow(tEnd - 1);
			if (titleRow == null)
				return false;

			String key = titles.get(titles.size() - 1).getName();
			int cntPosition = getColumnIndex(mergeAcrs, columnIndex, titleRow, key);
			if (cntPosition == -1) {
				return false;
			}
			else {
				position[i] = cntPosition;
			}
		}
		if (type == NodeType.ATTRIBUTE || type == NodeType.CODE || type == NodeType.NAME) {
			metaModelContext.metaPositionMapping.put(nodeKey, position);
		}
		else
			metaModelContext.parentPositionMapping.put(nodeKey, position);
		return true;
	}

	/**
	 * 知道合并的宽度，起始的列数，获取key所在的列数(根据规则，key只能在［起始列数，起始列数＋宽度］区间内)
	 * 
	 * @param mergeAcrs
	 * @param columnIndex
	 * @param titleRow
	 * @param key
	 * @return key所在的列数
	 */
	private int getColumnIndex(int mergeAcrs, short columnIndex, Row titleRow, String key) {

		short startColumnIndex = -1, endColumnIndex = -1;
		if (mergeAcrs > 0) {
			if (columnIndex > -1) {
				startColumnIndex = columnIndex;
				endColumnIndex = (short) (columnIndex + mergeAcrs);
			}
			else {
				// 没有找到
				return -1;
			}
		}
		else {
			// 不存在合并单元格的情况
			startColumnIndex = 0;
			endColumnIndex = titleRow.getLastCellNum();
		}
		boolean existInSheet = false;
		for (; startColumnIndex <= endColumnIndex; startColumnIndex++) {
			Cell cell = titleRow.getCell(startColumnIndex);
			if (cell == null)
				continue;

			String titleValue = POIExcelUtil.getCellValue(cell);
			if (titleValue.equals(key)) {
				existInSheet = true;
				break;
			}
		}
		if (existInSheet) {
			return startColumnIndex;
		}
		else {
			return -1;
		}
	}

	private int[] initPosition(int jointSize) {
		int[] position = new int[jointSize];
		for (int index = 0; index < position.length; index++) {
			// 初始化每一个位置
			position[index] = -1;
		}
		return position;
	}

	/**
	 * 获取列数
	 * 
	 * @param sheet
	 * @param titleName
	 * @param rowIndex
	 * @return 如存在则返回列数，如不存在则返回-1
	 */
	private short getColumnIndex(Sheet sheet, String titleName, int rowIndex) {
		Row cntTitleRow = sheet.getRow(rowIndex);
		if (cntTitleRow == null)
			return -1;

		short maxCellNum = cntTitleRow.getLastCellNum();
		for (short columnIndex = 0; columnIndex < maxCellNum; columnIndex++) {
			Cell cell = cntTitleRow.getCell(columnIndex);
			if (cell == null)
				continue;

			String titleValue = POIExcelUtil.getCellValue(cell);
			if (titleValue.equals(titleName)) { return columnIndex; }
		}
		return -1;
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
	 * 元素的类型，分别为元模型的代码，名称，祖先和属性
	 * 
	 * @author user
	 * @version 1.0 Date: Dec 27, 2010
	 * 
	 */
	private enum NodeType {
		CODE, NAME, FOREPARENT, ATTRIBUTE
	}

	/**
	 * 作为元模型间的依赖关系的上下文，包括角色，依赖端元模型树在SHEET中对应的位置；被依赖端元模型树在SHEET中对应的位置
	 * 其中元模型树有先后顺序要求
	 * 注意：该上下文只作用于每一个符合采集依赖关系要求的当前SHEET，不重用于下一个SHEET,即对于下一个SHEET，可能又是不同的配置了
	 * 
	 * @author user
	 * @version 1.0 Date: Dec 27, 2010
	 * 
	 */
	private class MetaDependencyContext {

		public static final byte OWNER_FLAG = 1, VALUE_FLAG = 2;

		String ownerRole, valueRole;

		Map<String, int[]> ownerPositionMapping = new LinkedHashMap<String, int[]>(0);

		Map<String, int[]> valuePositionMapping = new LinkedHashMap<String, int[]>(0);
	}

	// private class ValueDependencyErrorData {
	// // Map 元模型，SHEET,列，行
	// }

}
