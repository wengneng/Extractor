package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.model.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo.CognosBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse.impl.ParseUtil;

/**
 * @author 游林杰
 * 
 */
public class ParseModel {
	private Log log = LogFactory.getLog(ParseModel.class);
	private Document xmlDoc;
	private Namespace ns;
	// 默认的语言
	private String defaultLocal;
	// 模型BO
	private CognosBO modelCb;
	// XML解析范围
	private Map<String, ModelType> modelScope = new HashMap<String, ModelType>();
	private Map<String, List<String>> modelDependency = new HashMap<String, List<String>>();
	// 数据源
	private Map<String, CognosBO> dataSourceMap = new HashMap<String, CognosBO>();

	/**
	 * 模型解析总控制
	 * 
	 * @return ReportModelVO
	 * @roseuid 4A64153802FD
	 */
	public CognosBO parse(CognosBO cb, Document modelDoc) {
		this.modelCb = cb;
		this.xmlDoc = modelDoc;
		this.ns = xmlDoc.getRootElement().getNamespace();

		// 初始化XML解析范围
		// namespace
		ModelType namespace = new ModelType();
		namespace.setTypeName("namespace");
		namespace.setExpression(false);
		modelScope.put(namespace.getTypeName(), namespace);

		// folder
		ModelType folder = new ModelType();
		folder.setTypeName("folder");
		folder.setExpression(false);
		modelScope.put(folder.getTypeName(), folder);

		// querySubject
		ModelType querySubject = new ModelType();
		querySubject.setTypeName("querySubject");
		modelScope.put(querySubject.getTypeName(), querySubject);

		// queryItem
		ModelType queryItem = new ModelType();
		queryItem.setTypeName("queryItem");
		queryItem.getAttributes().add("datatype");
		queryItem.getAttributes().add("precision");
		queryItem.getAttributes().add("scale");
		queryItem.getAttributes().add("size");
		queryItem.getAttributes().add("nullable");
		modelScope.put(queryItem.getTypeName(), queryItem);

		// calculation
		ModelType calculation = new ModelType();
		calculation.setTypeName("calculation");
		calculation.setParseChildern(true);
		modelScope.put(calculation.getTypeName(), calculation);

		// filter
		ModelType filter = new ModelType();
		filter.setTypeName("filter");
		filter.setParseChildern(true);
		modelScope.put(filter.getTypeName(), filter);

		// shortCut
		ModelType shortCut = new ModelType();
		shortCut.setTypeName("shortCut");
		modelScope.put(shortCut.getTypeName(), shortCut);

		// 查询默认地区化设置
		this.defaultLocal = queryDefaultLocale(xmlDoc);
		cb.setCode(cb.getName());

		// 解析数据源
		parseDataSource();

		// 解析根名称空间,开始XML递归
		cb.getChildren().add(
				parseModel(xmlDoc.getRootElement().getChild("namespace", ns),
						cb, ""));

		cb.setDepExpressions(modelDependency);

		return cb;
	}

	/**
	 * 查询默认的地区化设置
	 * 
	 * @param modelDoc
	 * @return
	 */
	public String queryDefaultLocale(Document modelDoc) {

		// 默认地区化
		return ParseUtil.getChildElement(modelDoc,
				"/default:project/default:defaultLocale", ns).getValue();

	}

	/**
	 * 递归解析模型
	 * 
	 * @param el
	 *            标签
	 * @param parent
	 *            父节点
	 * @param path
	 *            父节点的表达式路径
	 * @return
	 */
	public CognosBO parseModel(Element el, CognosBO parent, String path) {

		// 构建BO
		CognosBO cb = buildBO(parent, el);

		// 解析属性
		buildArribute(el, cb);

		// 生成表达式
		path = buildExpression(el, path, cb);

		// 解析依赖关系
		List<Element> refObjs = ParseUtil.getChildrenElement(el,
				"./default:expression/default:refobj", ns);

		// 依赖表达式
		List<String> refExps = new ArrayList<String>();
		for (int i = 0; i < refObjs.size(); i++) {
			refExps.add(refObjs.get(i).getValue());
		}

		// 如果需要解析子孙节点，依赖关系在子孙节点建立，否则在本节点建立
		if (modelScope.get(el.getName()).isParseChildern()) {
			parseDepChildern(cb, path, refExps);
		} else {
			if (refExps.size() > 0) {
				modelDependency.put(path, refExps);
			}
		}

		if ("querySubject".equals(el.getName())) {
			ParseQuerySubject pqs = new ParseQuerySubject(xmlDoc, defaultLocal,
					ns);
			pqs.parseQuerySubject(cb, el, dataSourceMap);
		}

		if ("queryItem".equals(el.getName())) {
			ParseQueryItem pqi = new ParseQueryItem(xmlDoc, defaultLocal, ns);
			pqi.parseQueryItem(cb);
		}

		// 递归
		for (Iterator<String> it = modelScope.keySet().iterator(); it.hasNext();) {
			String key = (String) it.next();
			List<Element> els = el.getChildren(key, ns);
			for (int j = 0; j < els.size(); j++) {
				cb.getChildren().add(parseModel(els.get(j), cb, path));
			}
		}

		return cb;
	}

	/**
	 * 构建BO
	 * 
	 * @param parent
	 *            父节点
	 * @param el
	 *            标签
	 * @return
	 */
	private CognosBO buildBO(CognosBO parent, Element el) {
		CognosBO cb = new CognosBO(parent);

		// 名字
		String name = ParseUtil.getChildElement(el,
				"./default:name[@locale='" + defaultLocal + "']", ns).getText();

		// 包装BO
		cb.setCode(name);
		cb.setName(name);
		cb.setType(el.getName());

		return cb;
	}

	/**
	 * 解析属性,属性值在当前节点的子节点中,例如：<scale>0</scale>
	 */
	private void buildArribute(Element el, CognosBO cb) {

		List<String> attrs = modelScope.get(el.getName()).getAttributes();
		for (int i = 0; i < attrs.size(); i++) {
			Element attrEl = el.getChild(attrs.get(i), ns);
			if (attrEl != null) {
				cb.getAttributes().put(attrs.get(i), attrEl.getText());
			}
		}
	}

	/**
	 * 为报表引用，生成描述BO路径的表达式
	 * 
	 * @param el
	 * @param path
	 * @param cb
	 * @return
	 */
	private String buildExpression(Element el, String path, CognosBO cb) {
		// 如果当前是名称空间,需要修改表达式路径
		if ("namespace".equals(el.getName())) {
			path = "[" + cb.getName() + "]";
			return path;
		}

		// 表达式
		if (modelScope.get(el.getName()).isExpression()) {
			path = path + ".[" + cb.getName() + "]";
			cb.getAttributes().put("expression", path);
		}

		return path;
	}

	/**
	 * 解析数据源
	 */
	public void parseDataSource() {
		List<Element> dataSources = ParseUtil.getChildrenElement(xmlDoc,
				"/default:project/default:dataSources/default:dataSource", ns);
		for (int i = 0; i < dataSources.size(); i++) {
			Element el = dataSources.get(i);
			Element nameEl = el.getChild("name", ns);
			Element catalogEl = el.getChild("name", ns);
			Element schemaEl = el.getChild("name", ns);
			String name = "";
			String catalog = "CATALOG";
			String schema = "SCHEMA";
			if (nameEl != null && nameEl.getValue().length() > 0) {
				name = nameEl.getValue().toUpperCase();
			}
			if (catalog != null && catalogEl.getValue().length() > 0) {
				catalog = catalogEl.getValue().toUpperCase();
			}
			if (schemaEl != null && schemaEl.getValue().length() > 0) {
				schema = schemaEl.getValue().toUpperCase();
			}
			CognosBO root = modelCb.getRoot();
			if (nameEl != null) {
				// 查询数据源是否已经被解析过
				CognosBO dsCb = root.queryChildrenByCodeAndType(name,"dataSource");
				if (dsCb == null) {
					dsCb = new CognosBO(root);
					dsCb.setCode(name);
					dsCb.setName(name);
					dsCb.setType("dataSource");
					CognosBO catalogCb = new CognosBO(dsCb);
					CognosBO schemaCb = new CognosBO(catalogCb);

					catalogCb.setCode(catalog);
					catalogCb.setName(catalog);
					catalogCb.setType("catalog");

					schemaCb.setCode(schema);
					schemaCb.setName(schema);
					schemaCb.setType("schema");

					root.getChildren().add(dsCb);
					dsCb.getChildren().add(catalogCb);
					catalogCb.getChildren().add(schemaCb);
					this.dataSourceMap.put(dsCb.getCode(), schemaCb);
				} else {
					this.dataSourceMap.put(dsCb.getCode(), dsCb
							.queryChildrenByCodeAndType(catalog,"catalog").queryChildrenByCodeAndType(
									schema,"schema"));
				}
			}

		}

	}

	/**
	 * 对于filter,calculation等解析字段级元数据
	 */
	private void parseDepChildern(CognosBO parent, String parentExpression,
			List<String> refExpressions) {
		for (int i = 0; i < refExpressions.size(); i++) {
			String refExpression = refExpressions.get(i);
			String[] refExpList = ParseModelUtil
					.analysisExpression(refExpression);
			CognosBO cb = new CognosBO(parent);
			String code = refExpList[refExpList.length - 1];
			cb.setCode(code);
			cb.setName(code);
			cb.setType("dataItem");
			String expression = parentExpression + ".[" + code + "]";
			cb.getAttributes().put("expression", expression);

			// 依赖表达式
			List<String> refExps = new ArrayList<String>();
			refExps.add(refExpression);
			if (refExps.size() > 0) {
				modelDependency.put(expression, refExps);
			}

			parent.getChildren().add(cb);
		}
	}

}
