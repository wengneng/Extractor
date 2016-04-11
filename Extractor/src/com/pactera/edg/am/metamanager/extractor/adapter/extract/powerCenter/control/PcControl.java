package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.mapplet.ParseMappletInstance;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation.AbstractTransform;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation.JoinerTransform;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation.OutputTransform;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation.RouterTransform;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.transformation.UnionTransform;

public class PcControl {
	private Map<String, AbstractParse> parsers = new HashMap<String, AbstractParse>();
	private Map<String, AbstractTransform> transformParsers = new HashMap<String, AbstractTransform>();
	private AbstractTransform abstractTransform = new AbstractTransform();

	public PcControl() {
		transformParsers.put("Union Transformation", new UnionTransform());
		transformParsers.put("Joiner", new JoinerTransform());
		transformParsers.put("Router", new RouterTransform());
		transformParsers.put("Output Transformation", new OutputTransform());

	}

	// 解析控制器
	public PcBO parse(Document document, Map<String, String> varMap) {

		// 获取所有connection的名称
		getConnections(document);

		// 解析实例
		PcBO pcBO = parsePc(document.getRootElement(), null, varMap);

		// 解析关系
		parseDependency(document, pcBO);

		// 拷贝mapplet下的所有BO到mappletInstance下
		ParseMappletInstance parseMappletInstance = new ParseMappletInstance();
		parseMappletInstance.parse(pcBO, document);

		return pcBO;
	}

	/**
	 * 递归解析
	 * 
	 * @param element
	 * @param parentBO
	 * @return
	 */
	private PcBO parsePc(Element element, PcBO parent,
			Map<String, String> varMap) {

		PcBO pcBO = null;

		// 判断该类型是否在解析范围内
		if (ParseTypeHelper.existType(element.getName())) {
			AbstractParse abstractParse;
			// 获取该类型的解析器
			abstractParse = getParser(element.getName());

			// 解析
			pcBO = abstractParse.parse(parent, element, varMap);

			// 解析子节点
			if (pcBO != null) {
				List<Element> children = element.getChildren();
				for (int i = 0; i < children.size(); i++) {
					parsePc(children.get(i), pcBO, varMap);
				}
			}

		}
		return pcBO;
	}

	/**
	 * 获取该类型的解析器
	 * 
	 * @param type
	 * @return
	 */
	private AbstractParse getParser(String type) {

		if (parsers.get(type) != null) {
			return parsers.get(type);
		} else {
			// 获取该类型的解析器
			AbstractParse abstractParse;
			try {
				abstractParse = (AbstractParse) ParseTypeHelper.getPcType(type)
						.getParseClass().newInstance();
				parsers.put(type, abstractParse);
				return abstractParse;
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	// 获取所有的连接名称
	private void getConnections(Document document) {
		Map<String, String> map = new HashMap<String, String>();
		List<Element> els;
		try {
			els = XPath.selectNodes(document, "//CONNECTIONREFERENCE");
			System.out.println("共使用了" + els.size() + "次connection.");
			for (int i = 0; i < els.size(); i++) {
				Element el = els.get(i);
				String attValue = el.getAttributeValue("CONNECTIONNAME");
				if (attValue != null && !"".equals(attValue)) {
					map.put(attValue, attValue);
				}
			}
			System.out.println("共有" + map.size() + "个connection");
			for (Iterator iterator = map.keySet().iterator(); iterator
					.hasNext();) {
				String key = (String) iterator.next();
				System.out.println("connection name:" + key);
			}
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 解析依赖关系
	 * 
	 * @param document
	 * @param pcBO
	 */
	private void parseDependency(Document document, PcBO pcBO) {
		try {
			List<Element> instanceElements = (List<Element>) XPath.selectNodes(
					document, "//INSTANCE");
			for (int i = 0; i < instanceElements.size(); i++) {
				Element instanceElement = instanceElements.get(i);
				// 解析每个instance的依赖关系
				String transformationType = instanceElement
						.getAttributeValue("TRANSFORMATION_TYPE");
				PcBO instanceBO = pcBO.getAllPcBOs().get(
						PcUtils.genXPath(instanceElement));

				// 如果是客户自定义的类型，查询引用的转换类型
				if (transformationType.equals("Custom Transformation")) {
					Element depElement = instanceBO.getDepElement();
					transformationType = depElement
							.getAttributeValue("TEMPLATENAME");
				}

				getTransformParser(transformationType).genInstanceDependency(
						instanceBO, instanceElement);

			}
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 获取转换的解析器
	 * 
	 * @param TransformType
	 * @return
	 */
	private AbstractTransform getTransformParser(String TransformType) {
		if (transformParsers.get(TransformType) != null) {
			return transformParsers.get(TransformType);
		} else {
			return abstractTransform;
		}
	}

	public void setTransformParsers(
			Map<String, AbstractTransform> transformParsers) {
		this.transformParsers = transformParsers;
	}
}
