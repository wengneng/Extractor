package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl;

import java.io.IOException;
import java.io.StringReader;
import java.rmi.RemoteException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.cognos.developer.schemas.bibus._3.BaseClass;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.cognos.developer.schemas.bibus._3.Model;
import com.cognos.developer.schemas.bibus._3.OrderEnum;
import com.cognos.developer.schemas.bibus._3.PropEnum;
import com.cognos.developer.schemas.bibus._3.QueryOptions;
import com.cognos.developer.schemas.bibus._3.SearchPathMultipleObject;
import com.cognos.developer.schemas.bibus._3.Sort;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl.ParseUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl.ParseModel;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.report.impl.ParseReportFile;

public class Parse {
	private Log log = LogFactory.getLog(Parse.class);
	private ContentManagerService_Port cms;
	private CognosWebServerUtil cwsu=new CognosWebServerUtil();


	public ContentManagerService_Port getCms() {
		return cms;
	}

	public void setCms(ContentManagerService_Port cms) {
		this.cms = cms;
	}

	/**
	 * 解析所有的包
	 * 
	 * @return List
	 * @roseuid 4A64119A0128
	 */
	public CognosBO parse(CognosBO cb, ContentManagerService_Port cms) {
		this.cms = cms;
		List<CognosBO> children = cb.getChildren();
		for (int i = 0; i < children.size(); i++) {
			if ("model".equals(children.get(i).getType())) {
				log.info("开始解析cognos模型:"+children.get(i).getSearchPath());
				parseModel(children.get(i));
			}
			if ("report".equals(children.get(i).getType())) {
				log.info("开始解析cognos报表:"+children.get(i).getSearchPath());
				parseReport(children.get(i));
			}
			parse(children.get(i), cms);
		}

		return cb;
	}

	/**
	 * 解析包
	 * 
	 * @param packageBOs
	 * @return
	 */
	private CognosBO parseModel(CognosBO cb) {
		Document modelDoc = this.queryModel(cb.getSearchPath());
		if (modelDoc != null) {
			ParseModel pm = new ParseModel();
			// 解析模型
			cb = pm.parse(cb, modelDoc);
		}

		return cb;
	}

	/**
	 * 解析报表
	 * 
	 * @param reportVOList
	 */
	private void parseReport(CognosBO cb) {
		Document reportDoc = cwsu.queryReportDocument(cb.getSearchPath(),cms);
		ParseReportFile prf = new ParseReportFile();
		prf.parse(cb, reportDoc);
	}

	/**
	 * 查询包对应的模型
	 * 
	 * @param packageName
	 * @return
	 */
	public Document queryModel(String path) {
		// 查找对象的属性，名字和搜索路径
		// PropEnum枚举类
		PropEnum[] properties = { PropEnum.model, PropEnum.searchPath };

		// 返回对象的排序规则，按名字顺序排列
		Sort[] sortBy = { new Sort() };
		sortBy[0].setOrder(OrderEnum.ascending);

		// 查询选项，默认选项
		QueryOptions options = new QueryOptions();

		// 查询路径
		SearchPathMultipleObject spmo = new SearchPathMultipleObject();

		spmo.setValue(path);

		BaseClass[] results = null;
		try {
			results = cms.query(spmo, properties, sortBy, options);

			if (results.length > 0) {

				// 模型只有一个
				String modelString = ((Model) results[0]).getModel().getValue();

				// 反编码
				modelString = cwsu.deCodeXml(modelString);

				 System.out.println("modelXml"+modelString);
				// log.info("--------------------------------------------------");

				// jdom解析xml
				StringReader read = new StringReader(modelString);
				SAXBuilder sb = new SAXBuilder();
				Document modelDoc = sb.build(read);

				// XMLUtil.outputDocumentToFile(modelDoc, "d://111.xml");

				return modelDoc;
			} else {
				return null;
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}

}
