package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class RefDataSource {
//	private List<RelationalDataSourceBO> relationalDataSources = new ArrayList<RelationalDataSourceBO>();
//
//	/**
//	 * @param xmlDoc
//	 * @return List
//	 * @roseuid 4A555C1E032C
//	 */
//	public Map<String, List<?>> loadDataSource(Document xmlDoc) {
//		Map<String, List<?>> DataSourceMap = new HashMap<String, List<?>>();
////		System.out.println(xmlDoc.getRootElement());
//		// 关系型数据源
//		Namespace ns=xmlDoc.getRootElement().getNamespace();
//		List<?> relationalDataSourceEls = ParseUtil
//				.getChildrenElement(xmlDoc,
//						"/default:project/default:dataSources/default:dataSource[default:type/default:queryType='relational']", ns);
//
//		for (int i = 0; i < relationalDataSourceEls.size(); i++) {
//			Element relationalDataSourceEl = (Element) relationalDataSourceEls
//					.get(i);
//			RelationalDataSourceBO rds = new RelationalDataSourceBO();
//			rds.setCode(relationalDataSourceEl.getChildText("name",ns));
//			rds.setName(relationalDataSourceEl.getChildText("name",ns));
//			rds.setCatalog(relationalDataSourceEl.getChildText("catalog",ns));
//			rds.setSchema(relationalDataSourceEl.getChildText("schema",ns));
//			relationalDataSources.add(rds);
//		}
//		DataSourceMap.put("relational", relationalDataSources);
//
//		return DataSourceMap;
//	}
//
//	public List<RelationalDataSourceBO> getRelationalDataSources() {
//		return relationalDataSources;
//	}
//
//	public void setRelationalDataSources(
//			List<RelationalDataSourceBO> relationalDataSources) {
//		this.relationalDataSources = relationalDataSources;
//	}

}
