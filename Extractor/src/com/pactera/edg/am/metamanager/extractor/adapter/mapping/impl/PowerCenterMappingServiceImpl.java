package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.jdom.Document;
import org.jdom.input.SAXBuilder;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcDependency;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcControl;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcUtils;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

public class PowerCenterMappingServiceImpl extends BaseMappingServiceImpl
		implements IMetadataMappingService {
	private Log log = LogFactory.getLog(PowerCenterMappingServiceImpl.class);

	/**
	 * PowerCenter的XML文件存在的目录
	 */
	private String pcDirectory;

	/**
	 * 读取excel文件中的变量名与变量值
	 */
	private static Map<String, String> excelParametersCache = new HashMap<String, String>();

	// 存放转换后的元数据,key为bo的id
	private Map<String, MMMetadata> metaDataMap = new HashMap<String, MMMetadata>();

	// 元数据依赖关系
	private List<MMDDependency> dDependencies = new ArrayList<MMDDependency>();

	public void metadataMapping(AppendMetadata aMetadata) throws Exception {
		// 文件路径
		String path = AdapterExtractorContext.getInstance().getDsAbsolutePath();
		File f = new File(path);
		String pre = f.getName().substring(f.getName().lastIndexOf(".") + 1,
				f.getName().length());
		if (!pre.equals("xls")) {
			log.info(new StringBuilder("开始解析PowerCenter的.xml文件,文件绝对路径:")
					.append(path).append(" ,...").toString());
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO,
					new StringBuilder("开始解析PowerCenter的.xml文件,文件绝对路径:").append(
							path).append(" ,...").toString());

			try {

				MMMetaModel pcProjectMetaModel = singleParse(
						new FileInputStream(new File(path)), aMetadata, path);

				Set<MMMetaModel> aMetaModels = new HashSet<MMMetaModel>(1);
				aMetaModels.addAll(pcProjectMetaModel.getChildMetaModels());
				aMetadata.setChildMetaModels(aMetaModels);

				aMetadata.setDDependencies(dDependencies);
				log.info("文件解析完成!");
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO,
						"文件解析完成!");
			} catch (Exception e) {
				log.error("PowerCenter文件解析失败, 文件绝对路径:" + path, e);
				AdapterExtractorContext.addExtractorLog(
						ExtractorLogLevel.ERROR, "PowerCenter文件解析失败, 文件绝对路径:"
								+ path);
				throw e;
			}
		}
	}

	private MMMetaModel singleParse(InputStream inputStream,
			AppendMetadata aMetadata, String path) throws Exception {

		// 读取XML文件
		SAXBuilder sb = new SAXBuilder();

		sb.setValidation(false);
		sb.setEntityResolver(new EntityResolver() {
			public InputSource resolveEntity(String publicId, String systemId)
					throws SAXException, IOException {
				Resource r = new DefaultResourceLoader()
						.getResource("/com/pactera.edg.am/metamanager/extractor/adapter/extract/powerCenter/powrmart.dtd");
				return new InputSource(r.getInputStream());
			}
		});

		Document document = sb.build(inputStream);

		// 解析变量文件
		Map<String, String> varMap = excelParametersCache(path);

		/*
		 * 解析文件
		 */
		log.info("开始解析PowerCenter文件");
		long date1 = System.currentTimeMillis();

		PcControl pcControl = new PcControl();
		PcBO pcBO = pcControl.parse(document, varMap);

		long date2 = System.currentTimeMillis();
		log.info("解析完成，耗时：" + (date2 - date1) / 1000 + "秒");

		/*
		 * 转换BO
		 */
		log.info("开始转换BO");
		long date3 = System.currentTimeMillis();

		// 工程元模型
		MMMetaModel pcProjectMetaModel = aMetadata.getMetaModel();

		// 工程元数据
		MMMetadata pcProject = aMetadata.getMetadata();

		mappingPowerCenter(pcProjectMetaModel, pcProject, pcBO);

		long date4 = System.currentTimeMillis();
		log.info("转换完成，耗时：" + (date4 - date3) / 1000 + "秒");

		// 转换依赖关系
		log.info("开始转换依赖关系");
		long date5 = System.currentTimeMillis();
		mappingDependency(pcBO.getAllDependencys(), pcBO.getAllPcBOs());
		long date6 = System.currentTimeMillis();
		log.info("转换依赖关系完成，耗时：" + (date6 - date5) / 1000 + "秒");

		return pcProjectMetaModel;
	}

	public void mappingPowerCenter(MMMetaModel parentMetaModel,
			MMMetadata parentMetadata, PcBO pcBO) {

		// 如果没有元数据返回
		if (pcBO == null) {
			return;
		}

		// 模型
		String modelName = convertTypeToModelName(pcBO.getType());
		MMMetaModel mm = parentMetaModel.getChildMetaModel(modelName);

		if (mm == null) {
			mm = this.getMetaModel(parentMetaModel, modelName);
		}

		// 元数据
		MMMetadata md = this.getMetaData(parentMetadata, mm, pcBO.getCode(),
				pcBO.getName(), pcBO.getId());

		// 属性
		md.setAttrs(pcBO.getAttributes());

		// 递归
		for (int i = 0; i < pcBO.getChildren().size(); i++) {
			mappingPowerCenter(mm, md, pcBO.getChildren().get(i));
		}

	}

	/**
	 * 将标签名转换为模型名
	 * 
	 * @param type
	 * @return
	 */
	private String convertTypeToModelName(String type) {
		Map<String, String> modelMap = new HashMap<String, String>();
		modelMap.put("SOURCEINSTANCE", "SourceInstance");
		modelMap.put("TARGETINSTANCE", "TargetInstance");
		modelMap.put("FLATFILE", "FlatFile");
		modelMap.put("FLATFILECOLUMN", "FlatFileColumn");
		modelMap.put("FLATFILECOLUMN", "FlatFileColumn");
		modelMap.put("COLUMNSET", "ColumnSet");
		modelMap.put("DBCOLUMN", "DbColumn");
		modelMap.put("TRANSFORMATIONINSTANCE", "TransformationInstance");
		modelMap.put("TRANSFORMFIELDINSTANCE", "TransformfieldInstance");
		modelMap.put("MAPPLETINSTANCE", "MappletInstance");
		modelMap.put("POWERMART", "PowerMart");
		modelMap.put("MAPPLETINSTANCE", "MappletInstance");

		if (modelMap.get(type) != null) {
			return "Pc86" + modelMap.get(type);
		} else {
			return "Pc86" + type.substring(0, 1).toUpperCase()
					+ type.substring(1).toLowerCase();
		}

	}

	/**
	 * 得到一个元模型，包含名称，代码和父元模型
	 * 
	 * @param parentMetaModel
	 * @param clazz
	 * @return
	 */
	private MMMetaModel getMetaModel(MMMetaModel parentMetaModel, String mmName) {

		MMMetaModel metaModel = new MMMetaModel();
		metaModel.setName(mmName);
		metaModel.setCode(mmName);
		metaModel.setParentMetaModel(parentMetaModel);

		// 对父模型进行设置
		parentMetaModel.setHasChildMetaModel(true);
		parentMetaModel.addMetaModel(metaModel);

		return metaModel;
	}

	/**
	 * 得到一个元数据，包含名称和父元数据
	 * 
	 * @param parentMetadata
	 * @param metaDataName
	 * @return
	 */
	private MMMetadata getMetaData(MMMetadata parentMetadata, MMMetaModel mm,
			String metaDataCode, String metaDataName, String id) {
		MMMetadata mMMetadata = super.createMetadata(parentMetadata, mm,
				metaDataCode, metaDataName);
		metaDataMap.put(id, mMMetadata);
		return mMMetadata;
	}

	/**
	 * 从EXCEL中获取变量名与变量值
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private Map<String, String> excelParametersCache(String path)
			throws IOException {
		if (path.lastIndexOf("\\") != -1) {
			path = path.substring(0, path.lastIndexOf("\\"));
		} else {
			path = path.substring(0, path.lastIndexOf("/"));
		}
		String filePath = path + "/powerCenterMapping.xls";
		// 读取excel
		FileInputStream fis = new FileInputStream(filePath); // 根据excel文件路径创建文件流
		POIFSFileSystem fs = new POIFSFileSystem(fis); // 利用poi读取excel文件流
		HSSFWorkbook wb = new HSSFWorkbook(fs); // 读取excel工作簿
		HSSFSheet sheet = wb.getSheetAt(0);
		for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
			HSSFRow row = sheet.getRow(i); // 取出sheet中的某一行数据
			if (row != null) {
				int j = 0;
				HSSFCell cell = row.getCell((short) j); // 获取该行中的一个单元格对象
				HSSFCell cell2 = row.getCell((short) j + 1);
				excelParametersCache.put(cell.getStringCellValue()
						.toUpperCase(), cell2.getStringCellValue()
						.toUpperCase());
			}
		}
		return excelParametersCache;
	}

	/**
	 * 转换所有BO的依赖关系
	 */
	private void mappingDependency(Map<String, PcDependency> dependencys,
			Map<String, PcBO> allPcBOs) {
		Map<String, String> dependencyMap = new HashMap<String, String>();
		for (Iterator<String> iterator = dependencys.keySet().iterator(); iterator
				.hasNext();) {
			PcDependency pcDependency = dependencys.get(iterator.next());
			addMMDependency(pcDependency, dependencyMap, true);

			// 表级
			if (allPcBOs.get(pcDependency.getFromId()).getType().equals(
					"TRANSFORMFIELDINSTANCE")) {
				String fromParentId = allPcBOs.get(pcDependency.getFromId())
						.getParent().getId();
				String toParentId = allPcBOs.get(pcDependency.getToId())
						.getParent().getId();
				PcDependency tablePcDependency = new PcDependency(fromParentId,
						pcDependency.getFromRole(), toParentId, pcDependency
								.getToRole());

				addMMDependency(tablePcDependency, dependencyMap, false);

			}

		}
	}

	/**
	 * 添加依赖
	 * 
	 * @param pcDependency
	 * @param dependencyMap
	 */
	private void addMMDependency(PcDependency pcDependency,
			Map<String, String> dependencyMap, boolean isLog) {
		MMMetadata formMm = metaDataMap.get(pcDependency.getFromId());
		MMMetadata toMm = metaDataMap.get(pcDependency.getToId());
		if (formMm != null && toMm != null) {
			String dependencyKey = PcUtils.genDependencyKey(pcDependency);
			if (dependencyMap.get(dependencyKey) == null) {
				addDependency(formMm, pcDependency.getFromRole(), toMm,
						pcDependency.getToRole());
				dependencyMap.put(dependencyKey, dependencyKey);
			} else {
				if (isLog) {
					log.debug("重复的依赖关系：" + dependencyKey);
				}
			}
		} else {
			log.debug("查询到的mm为空：fromId:" + pcDependency.getFromId() + ",toId:"
					+ pcDependency.getToId());
		}
	}

	/**
	 * 生成依赖关系对象
	 * 
	 * @param modelDependencyMap
	 */
	private void addDependency(MMMetadata owner, String ownerRole,
			MMMetadata value, String valueRole) {
		MMDDependency mmd = new MMDDependency();
		mmd.setOwnerMetadata(owner);
		mmd.setValueMetadata(value);
		mmd.setOwnerRole(ownerRole);
		mmd.setValueRole(valueRole);
		dDependencies.add(mmd);

	}

	public String getPcDirectory() {
		return pcDirectory;
	}

	public void setPcDirectory(String pcDirectory) {
		this.pcDirectory = pcDirectory;
	}

}
