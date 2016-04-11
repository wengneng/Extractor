package com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsConfig;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.IJiCaiDBExtractService;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.db.impl.AbstractJiCaiDBExtractService;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.util.HSqlDB;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.jicai.vo.SystemVO;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl.PoiExcelTemplateMappingServiceImpl;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util.JiCaiExcelUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util.MMMetadataUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util.PropertiesUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util.TableFiltrate;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.control.impl.ExtractorServiceImpl;
import com.pactera.edg.am.metamanager.extractor.increment.IIncrementAnalysisService;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;

/**
 * 处理规则：按照元模型树型结构自底向上处理，数据以后进的为准 元数据项目：如果SHEET名称以＂删除＂开头，则该SHEET跳过不处理;
 * 同时将"删除"SHEET的数据，添加至删除列表中，供数据删除使用
 * 
 * @author user
 * @version 1.0 Date: Dec 23, 2010
 * 
 */
public class JiCaiDBMappingServiceImpl extends PoiExcelTemplateMappingServiceImpl implements
		IMetadataMappingService {

	/**
	 * DB数据源获取接口
	 */
	private IJiCaiDBExtractService dbDao;
	/**
	 * 需要采集的SCHEMA,需采多个时,用","分隔
	 */
	private List<Schema> listSch;
	private String schemas;
	
	public void setSchemas(String schemas) {
		this.schemas = schemas;
	}

	protected String getExtractSchemas() {
		return schemas;
	}

	public static final int maxCount = 60000;
	private Log log = LogFactory.getLog(JiCaiDBMappingServiceImpl.class);



	private List<Schema> getSchemas() throws Exception {
		List<Schema> schemas = new ArrayList<Schema>();
		try {
			String[] extractSchemas = getExtractSchemas().toUpperCase().split(
					"\\s*,\\s*");
			for (String string : extractSchemas) {
				Schema schema = new Schema();
				schema.setName(string.toUpperCase());
				schemas.add(schema);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return schemas;
	}

	public void metadataMapping(AppendMetadata aMetadata) throws Exception {
		// 获取模板编号，取得模板配置
		IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance()
				.getIClassifier();
		IIncrementAnalysisService incrementAnalyzer = (IIncrementAnalysisService) ExtractorContextLoader
				.getBean(IIncrementAnalysisService.INCREMENT_SPRING_NAME);
		try {

			String dsId = (String) PropertiesUtil.pro.get("cgb_dataSource_id");
			
			if(dsId ==null){
				dsId = AdapterExtractorContext.getInstance().getDatasourceId();
			}
			List<TemplateClsConfig> tcConfigs = iClassifier
					.getTemplateClsConfigsByDs(dsId);
//			Map<String,String> cTplTitle = iClassifier.getTplMapInfo(dsId);
			//获取需要我们需要采集的Schema
			listSch = getSchemas();
			log.info("【DB集采】DB集采每次最大采集数为:" + maxCount );
			for (int i = 0; listSch != null && i < listSch.size(); i++) {
				Schema schema = listSch.get(i);
				String schName = schema.getName();
				//初始化内存数据库，根据schname名称初始化内存数据，每次生成会清除上次数据库内容
				//必须先初始化内存数据库，内存数据库处理方式为文件加载方式
				dbDao.initHSQLDB(schName);
				log.info("schema:" + schema.getName());
				//获取表级信息长度
				int tablesSize = dbDao.getTableSize(schName);
				//获取字段级信息长度
				int fieldsSzie = dbDao.getFieldSize(schName);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "获取表级长度："+tablesSize);
				AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.INFO, "获取字段级长度："+fieldsSzie);
				int count = tablesSize > fieldsSzie ? tablesSize : fieldsSzie;
				tablesSize = (tablesSize % maxCount == 0 ? 0 : 1) + (tablesSize / maxCount);
				count = (count % maxCount == 0 ? 0 : 1) + (count / maxCount);
				int start = 0, limit = maxCount;
				for (int j = 0; j < count; j++) {
					super.aMetadata = aMetadata;
					boolean flag = true;
					if(j<tablesSize){
						flag = true;
					}else{
						flag = false;
					}
					SystemVO sysInfos = dbDao
							.getSysInfos(schName, start, limit,flag);
					log.info("【DB集采】开始对数据筛选过滤操作！");
					//数据筛选处理
					TableFiltrate.getInstance().filtrate(sysInfos);
					log.info("【DB集采】结束对数据筛选过滤操作！");
					super.wBook = JiCaiExcelUtil.init().getHSSFWorkbook(sysInfos);
					
					super.iterTemplateConf(tcConfigs);

					super.setDependencies(tcConfigs);

					aMetadata.setChildMetaModels(aMetadata.getMetaModel()
							.getChildMetaModels());
					aMetadata.setDDependencies(super.dependencies);

					super.afterExtracted();

					super.printExceptionData();

					if (i < (listSch.size() - 1)
							|| (i == (listSch.size() - 1) && j < (count - 1))) {
						clear();
						MMMetadataUtil.init().setMetadataCover(tcConfigs);
						//执行分析、入库操作
						ExtractorServiceImpl.execute(incrementAnalyzer, aMetadata);
						AdapterExtractorContext.getInstance().getIsCover().clear();
//						if (AdapterExtractorContext.getInstance().isFullIncrementCompare()) {
							// 对于增量的数据,才需要删除
							ExtractorServiceImpl.batchDelete();
//						}
					}else{
						MMMetadataUtil.init().setMetadataCover(tcConfigs);
					}
					start = limit + 1;
					limit = limit + maxCount;
					sysInfos = null;
				}
				//当执行完一个schema采集任务后，关闭内存数据库，下个循环将重新生成
				HSqlDB.Shutdown();
				System.gc();
			}
		} catch (Exception e) {
			log.error("【DB集采】解析失败, 请确保DB采集数据的完整性！" + e);
			AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.ERROR,
					"解析失败, 请确保DB采集数据的完整性！");
			HSqlDB.Shutdown();
			System.gc();
			throw e;
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			HSqlDB.Shutdown();
			System.gc();
			e.printStackTrace();
		} finally {
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

	public void setDbDao(IJiCaiDBExtractService dbDao) {
		this.dbDao = dbDao;
	}

}
