package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.parse;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Attribute;
import org.jdom.Element;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.ParseTypeHelper;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcUtils;

public class AbstractParse {
	private static Log log = LogFactory.getLog(AbstractParse.class);

	/**
	 * 解析
	 * 
	 * @param parent
	 * @param element
	 * @param varMap
	 *            外部变量替换
	 */
	public PcBO parse(PcBO parent, Element element, Map<String, String> varMap) {
		PcBO pcBO = new PcBO(parent, element, null);

		// 组装基本信息
		bulidBaseInfoFromElement(pcBO, element);

		// 解析属性
		buildAttribute(pcBO, element);

		// 添加依赖关系
		buildDependency(pcBO, element);

		return pcBO;
	}

	/**
	 * 组装基本信息
	 * 
	 * @param pcBO
	 * @param element
	 * @return
	 */
	protected void bulidBaseInfoFromElement(PcBO pcBO, Element element) {
		// log.info("bulidBaseInfoFromElement");
		String name = null;
		// 如果没有名字，取标签名
		if (element.getAttributeValue("NAME") == null) {
			name = element.getName();
		} else {
			name = element.getAttributeValue("NAME");
		}
		pcBO.setCode(name);
		pcBO.setName(name);
		pcBO.setType(element.getName());
	}

	/**
	 * 属性装箱
	 * 
	 * @param pcBO
	 * @param element
	 */
	protected void buildAttribute(PcBO pcBO, Element element) {
		// log.info("buildAttribute");
		List<Attribute> attributes = element.getAttributes();
		for (int i = 0; i < attributes.size(); i++) {
			pcBO.getAttributes().put(attributes.get(i).getName().toLowerCase(),
					attributes.get(i).getValue());

		}
	}

	/**
	 * 根据instance中的描述，添加依赖关系
	 * 
	 * @param bofBO
	 * @param documentTreeNode
	 */
	protected void buildDependency(PcBO pcBO, Element element) {
		// log.info("buildDependency");
		if (ParseTypeHelper.getPcType(element.getName()).isParseDependency()) {
			String reUseAble = element.getAttributeValue("REUSABLE");
			String dbdName = element.getAttributeValue("DBDNAME");
			String type = element.getAttributeValue("TYPE");
			String transformationName = element
					.getAttributeValue("TRANSFORMATION_NAME");

			Element depedElement;
			if ("NO".equals(reUseAble)) {
				// 如果是非重用的，在当前MAPPING下
				depedElement = PcUtils.getChildByName(element
						.getParentElement(), type, transformationName, dbdName);
			} else {
				// 如果是可重用的，放在文件夹下
				depedElement = PcUtils.getChildByName(element
						.getParentElement().getParentElement(), type,
						transformationName, dbdName);
			}

			if (depedElement != null) {
				// 添加依赖关系
				pcBO.setElementDependency(depedElement);
			} else {
				log.info("找不到该instance的定义：reUseAble=" + reUseAble + " dbdName:"
						+ dbdName + " type:" + type + " transformationName:"
						+ transformationName);
			}
		}
	}
}
