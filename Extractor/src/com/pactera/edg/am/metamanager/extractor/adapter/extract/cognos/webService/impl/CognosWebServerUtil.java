package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.webService.impl;

import java.io.IOException;
import java.io.StringReader;
import java.rmi.RemoteException;

import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.cognos.developer.schemas.bibus._3.BaseClass;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.cognos.developer.schemas.bibus._3.OrderEnum;
import com.cognos.developer.schemas.bibus._3.PropEnum;
import com.cognos.developer.schemas.bibus._3.QueryOptions;
import com.cognos.developer.schemas.bibus._3.Report;
import com.cognos.developer.schemas.bibus._3.SearchPathMultipleObject;
import com.cognos.developer.schemas.bibus._3.Sort;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class CognosWebServerUtil {

	/**
	 * @return String
	 * @roseuid 4A52B74202DE
	 */
	public String deCodeXml(String xmlString) {
		xmlString.replace("&lt;", "<");
		xmlString.replace("&gt;", ">");
		xmlString.replace("&quot;", "\"");
		xmlString.replace("&apos;", "'");
		xmlString.replace("&amp;", "&");
		return xmlString;
	}
	
	/**
	 * 查询报表的XML
	 * 
	 * @param serachPath
	 * @return
	 */
	public Document queryReportDocument(String searchPath,ContentManagerService_Port cms) {
		// 查找对象的属性，名字和搜索路径
		// PropEnum枚举类
		PropEnum[] properties = { PropEnum.specification, PropEnum.defaultName };

		// 返回对象的排序规则，按名字顺序排列
		Sort[] sortBy = { new Sort() };
		sortBy[0].setOrder(OrderEnum.ascending);

		// 查询选项，默认选项
		QueryOptions options = new QueryOptions();

		// 查询路径
		SearchPathMultipleObject spmo = new SearchPathMultipleObject();

		spmo.setValue(searchPath);

		BaseClass[] results = null;
		try {
			results = cms.query(spmo, properties, sortBy, options);

			// 模型只有一个
			String reportString = ((Report) results[0]).getSpecification()
					.getValue();

			// 反编码
			reportString = this.deCodeXml(reportString);

//			 log.info("reportXml:"+reportString);
			// log.info("--------------------------------------------------");

			// jdom解析xml
			StringReader read = new StringReader(reportString);
			SAXBuilder sb = new SAXBuilder();
			Document modelDoc = sb.build(read);
			return modelDoc;
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
}
