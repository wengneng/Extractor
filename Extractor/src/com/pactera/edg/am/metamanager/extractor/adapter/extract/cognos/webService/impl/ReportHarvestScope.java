package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;

import com.cognos.developer.schemas.bibus._3.BaseClass;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.cognos.developer.schemas.bibus._3.OrderEnum;
import com.cognos.developer.schemas.bibus._3.PropEnum;
import com.cognos.developer.schemas.bibus._3.QueryOptions;
import com.cognos.developer.schemas.bibus._3.SearchPathMultipleObject;
import com.cognos.developer.schemas.bibus._3.Sort;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl.ParseUtil;

/**
 * 读取采集目录
 * 
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class ReportHarvestScope {
	public static Log log = LogFactory.getLog(ReportHarvestScope.class);
	private ContentManagerService_Port cms;
	private String parseType = ",package,folder,model,report,";
	private CognosWebServerUtil cwsu = new CognosWebServerUtil();
	private Map<String, String> usedModelMap = new HashMap<String, String>();
	private List<CognosBO> allModelBO = new ArrayList<CognosBO>();

	/**
	 * @return com.pactera.edg.am.cognos.vo.ReportPackage
	 * @roseuid 4A52B85C0148
	 */
	public CognosBO loadCatalogFromServer(ContentManagerService_Port cms) {
		log
				.info("--------------------------------------------------------------------------");
		log.info("开始读取cognos采集目录");
		Date date1 = new Date();
		this.setCms(cms);

		// 初始化目录
		CognosBO cb = boxCatalog();

		Date date2 = new Date();
		log.info("读取cognos采集目录,耗时：" + (date2.getTime() - date1.getTime())
				/ 1000 + "秒");
		log.info("结束读取cognos采集目录");

		return cb;
	}

	/**
	 * 通过查询路径查询
	 * 
	 * @param searchPath
	 * @return
	 */
	private BaseClass[] query(String searchPath) {
		// 查找对象的属性，名字和搜索路径
		// PropEnum枚举类
		PropEnum[] properties = { PropEnum.defaultName, PropEnum.searchPath,
				PropEnum.hasChildren, PropEnum.type };

		// 返回对象的排序规则，按名字顺序排列
		Sort[] sortBy = { new Sort() };
		sortBy[0].setOrder(OrderEnum.ascending);
		sortBy[0].setPropName(PropEnum.defaultName);

		// 查询选项，默认选项
		QueryOptions options = new QueryOptions();

		// 查询路径
		SearchPathMultipleObject spmo = new SearchPathMultipleObject();

		spmo.setValue(searchPath);

		BaseClass[] results = null;
		try {
			results = cms.query(spmo, properties, sortBy, options);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
	}

	/**
	 * 查询内容库
	 * 
	 * @param results
	 * @return
	 */
	private CognosBO boxCatalog() {
		CognosBO cb = new CognosBO(null);
		cb.setCode("content");
		cb.setName("content");
		cb.setChildren(boxChilden("/content", cb));		
		//去掉无用的模型
		removeUnUsedModel();
		return cb;
	}

	private List<CognosBO> boxChilden(String parentPath, CognosBO parent) {
		String currentPath = parentPath + "/*";
		BaseClass[] results = query(currentPath);
		for (int j = 0; j < results.length; j++) {
			String searchPath = results[j].getSearchPath().getValue()
					.toString();
			String typeTemp = searchPath.substring(
					searchPath.lastIndexOf("/") + 1, searchPath.length());
			String type = typeTemp;
			if (typeTemp.indexOf("[") != -1) {
				type = typeTemp.substring(0, typeTemp.indexOf("["));
			}

			// 获取报表用到的模型路径
			if ("report".equals(type)) {
				String modelPath = this.getModelPath(searchPath);
				this.usedModelMap.put(modelPath, modelPath);
			}

			if (parseType.indexOf("," + type + ",") != -1) {
				CognosBO cb = new CognosBO(parent);
				cb.setCode(results[j].getDefaultName().getValue().toString());
				cb.setName(results[j].getDefaultName().getValue().toString());
				cb.setSearchPath(searchPath);
				cb.setType(type);
				parent.getChildren().add(cb);

				// 缓存所有的model bo
				if ("model".equals(type)) {
					allModelBO.add(cb);
				}

				log.info("读取cognos目录路径:" + cb.getSearchPath());
				if (results[j].getHasChildren().isValue()) {
					cb.setChildren(boxChilden(cb.getSearchPath(), cb));
				}
			}
		}
		return parent.getChildren();
	}

	// 判断当前模型是否是最新的版本,如果是，删除旧版本
	public Boolean isModelNew(String modelName, CognosBO parentBO) {
		for (int i = 0; i < parentBO.getChildren().size(); i++) {
			CognosBO bo = parentBO.getChildren().get(i);
			if ("model".equals(bo.getType())) {
				String modelNameTemp = bo.getName();
				if (ParseUtil.isDateAfter(modelNameTemp,
						modelName)) {
					return false;
				} 
			}
		}
		// 没有可比的是最新的
		return true;
	}

	/**
	 * 获取报表的模型路径
	 * 
	 * @param searchPath
	 * @return
	 */
	private String getModelPath(String searchPath) {
		Document reportDoc = cwsu.queryReportDocument(searchPath, cms);
		Element modelPathEl = reportDoc.getRootElement().getChild("modelPath",
				reportDoc.getRootElement().getNamespace());
		return modelPathEl.getValue();
	}

	/**
	 * 去掉报表未使用的历史模型
	 */
	private void removeUnUsedModel() {
		for (int i = 0; i < allModelBO.size(); i++) {
			CognosBO cb = allModelBO.get(i);
			//该模型不是最新的版本，并且没有被使用过，去掉
			if(!isModelNew(cb.getCode(), cb.getParent())&&usedModelMap.get(cb.getSearchPath())==null){
				//删除自己
				cb.getParent().getChildren().remove(cb);
			}
		}
	}

	public static Log getLog() {
		return log;
	}

	public static void setLog(Log log) {
		ReportHarvestScope.log = log;
	}

	public ContentManagerService_Port getCms() {
		return cms;
	}

	public void setCms(ContentManagerService_Port cms) {
		this.cms = cms;
	}

	public String getParseType() {
		return parseType;
	}

	public void setParseType(String parseType) {
		this.parseType = parseType;
	}

	public List<CognosBO> getAllModelBO() {
		return allModelBO;
	}

	public void setAllModelBO(List<CognosBO> allModelBO) {
		this.allModelBO = allModelBO;
	}

}
