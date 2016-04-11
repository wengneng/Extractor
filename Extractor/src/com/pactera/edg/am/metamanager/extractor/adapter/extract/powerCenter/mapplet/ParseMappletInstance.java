package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.mapplet;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcDependency;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcUtils;

/**
 * 解析mapplet实例
 * 
 * @author Administrator
 * 
 */
public class ParseMappletInstance {
	private Log log = LogFactory.getLog(getClass());

	public void parse(PcBO pcBO, Document document) {
		try {
			// 所有mapplet实例
			List<Element> mappletInstanceElements = (List<Element>) XPath
					.selectNodes(document, "//INSTANCE[@TYPE='MAPPLET']");

			for (int i = 0; i < mappletInstanceElements.size(); i++) {
				Element mappletInstanceElement = mappletInstanceElements.get(i);
				String mappletInstanceId = PcUtils
						.genXPath(mappletInstanceElement);
				String reUseAble = mappletInstanceElement
						.getAttributeValue("REUSABLE");
				String mappletName = mappletInstanceElement
						.getAttributeValue("TRANSFORMATION_NAME");
				String mappletXpath = "./MAPPLET[@NAME='" + mappletName + "']";
				// mapplet定义
				Element mappletElement;
				if ("YES".equals(reUseAble)) {
					mappletElement = (Element) XPath.selectSingleNode(
							mappletInstanceElement.getParentElement()
									.getParentElement(), mappletXpath);
				} else {
					mappletElement = (Element) XPath.selectSingleNode(
							mappletInstanceElement.getParentElement(),
							mappletXpath);
				}

				String mappletId = PcUtils.genXPath(mappletElement);
				// mapplet的BO
				PcBO mappletBO = pcBO.getAllPcBOs().get(mappletId);

				// 构建mappletInstanceBO
				PcBO mappletInstanceBO = pcBO.getAllPcBOs().get(
						PcUtils.genXPath(mappletInstanceElement));
				// 依赖的标签
				mappletInstanceBO.setElementDependency(mappletElement);
				/*
				 * 复制所有的mapplet的bo到该mappletInstance下
				 */
				copyBO(mappletBO, mappletInstanceBO, mappletInstanceId);
				
				//mappletInstance下的instance与其他mapping下的instance之间的关系
				parseMappletDependency(mappletInstanceBO, mappletElement, mappletInstanceElement);
			}

		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 拷贝mapplet下的BO到mappletInstance下，并且修改对应的ID
	 * 
	 * @param mappletBO
	 * @param mappletInstanceId
	 */
	private void copyBO(PcBO pcBO, PcBO copyeBO, String mappletInstanceId) {
		for (int i = 0; i < pcBO.getChildren().size(); i++) {
			PcBO childBO = pcBO.getChildren().get(i);
			// 复制
			PcBO childCopyBO = (PcBO) childBO.clone();
			// 重新设置id
			childCopyBO.setId(mappletInstanceId + childBO.getId());

			// 父节点
			childCopyBO.setParent(copyeBO);
			// 父节点的子节点
			copyeBO.getChildren().add(childCopyBO);
			// 所有的BO集合
			childCopyBO.getAllPcBOs().put(childCopyBO.getId(), childCopyBO);
			// 清空子节点
			childCopyBO.setChildren(new ArrayList<PcBO>());

			// 依赖关系
			// 定义的依赖关系
			Element depElement = childBO.getDepElement();
			if (depElement != null) {
				childCopyBO.setElementDependency(depElement);
			}
			// connector依赖关系
			List<PcDependency> dependencys = childBO.getDependencys();
			childCopyBO.setDependencys(new ArrayList<PcDependency>());

			for (int j = 0; j < dependencys.size(); j++) {
				PcDependency pcDependency = dependencys.get(j);
				childCopyBO.addDependency(mappletInstanceId
						+ pcDependency.getToId(), pcDependency.getToRole());
			}

			copyBO(childBO, childCopyBO, mappletInstanceId);
		}
	}

	/**
	 * 建立mappletInstance下BO与Mapping下的BO的关系
	 * 
	 * @param mappletInstanceBO
	 * @param mappletElement
	 */
	private void parseMappletDependency(PcBO mappletInstanceBO,
			Element mappletElement, Element mappletInstanceElement) {

		try {
			// mapplet的name
			String mappletName = mappletElement
					.getAttributeValue("NAME");

			// mapplet中的mapllet转换
			Element mappletTransformation = (Element) XPath.selectSingleNode(
					mappletElement, "./TRANSFORMATION[@NAME='" + mappletName
							+ "' and @TYPE ='Mapplet']");

			// 找出所有使用了mapplet输出的连接
			List<Element> outputConnectorElements = (List<Element>) XPath
					.selectNodes(mappletInstanceElement,
							"../CONNECTOR[@FROMINSTANCE ='"
									+ mappletInstanceElement
											.getAttributeValue("NAME") + "']");

			for (int i = 0; i < outputConnectorElements.size(); i++) {
				Element connectorElement = outputConnectorElements.get(i);
				String fromField = connectorElement
						.getAttributeValue("FROMFIELD");
				String toInstance = connectorElement
						.getAttributeValue("TOINSTANCE");
				String toField = connectorElement.getAttributeValue("TOFIELD");
				String toInstanceType = connectorElement
						.getAttributeValue("TOINSTANCETYPE");

				// 查询依赖的字段
				PcBO refFieldBO = findRefFieldBO(mappletInstanceBO,
						mappletTransformation, fromField);
				PcBO toInstanceBO = mappletInstanceBO.getParent()
						.searchChildByCodeAndType(toInstance,
								genType(toInstanceType));
				List<PcBO> toFieldBOList = new ArrayList<PcBO>();
				if ("Target Definition".equals(toInstanceType)) {
					toFieldBOList = PcUtils.getFieldBO(toInstanceBO, toField);
				} else {
					toFieldBOList.add(toInstanceBO.searchChildByCodeAndType(
							toField, "TRANSFORMFIELDINSTANCE"));
				}

				// 依赖关系
				addDependency(refFieldBO, toFieldBOList, toInstanceType);

			}

			// 找出所有使用了mapplet输入的连接
			List<Element> intputConnectorElements = (List<Element>) XPath
					.selectNodes(mappletInstanceElement,
							"../CONNECTOR[@TOINSTANCE ='"
									+ mappletInstanceElement
											.getAttributeValue("NAME") + "']");
			for (int i = 0; i < intputConnectorElements.size(); i++) {
				Element connectorElement = intputConnectorElements.get(i);
				String fromField = connectorElement
						.getAttributeValue("FROMFIELD");
				String fromInstance = connectorElement
						.getAttributeValue("FROMINSTANCE");
				String toField = connectorElement.getAttributeValue("TOFIELD");

				// 查询依赖的字段
				PcBO refFieldBO = findRefFieldBO(mappletInstanceBO,
						mappletTransformation, toField);
				PcBO fromFieldBO = mappletInstanceBO.getParent()
						.searchChildByCodeAndType(fromInstance,
								"TRANSFORMATIONINSTANCE")
						.searchChildByCodeAndType(fromField,
								"TRANSFORMFIELDINSTANCE");
				List<PcBO> toFieldBOList = new ArrayList<PcBO>();
				toFieldBOList.add(refFieldBO);
				// 依赖关系
				addDependency(fromFieldBO, toFieldBOList,
						"TRANSFORMATIONINSTANCE");

			}

		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 找出mappletInstance相关联的字段
	 * 
	 * @param mappletInstanceBO
	 * @param mappletTransformation
	 * @param fieldName
	 * @return
	 */
	private PcBO findRefFieldBO(PcBO mappletInstanceBO,
			Element mappletTransformation, String fieldName) {
		Element mappletField;
		try {
			mappletField = (Element) XPath.selectSingleNode(
					mappletTransformation, "./TRANSFORMFIELD[@NAME='"
							+ fieldName + "']");

			if (mappletField != null) {
				String refField = mappletField.getAttributeValue("REF_FIELD");
				String refInstance = mappletField
						.getAttributeValue("MAPPLETGROUP");

				return mappletInstanceBO.searchChildByCodeAndType(refInstance,
						"TRANSFORMATIONINSTANCE").searchChildByCodeAndType(
						refField, "TRANSFORMFIELDINSTANCE");
			} else {
				log.error("找不到mapplet对应的字段,fieldName:" + fieldName);
			}
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * 生成类型
	 * 
	 * @param instanceType
	 * @return
	 */
	private String genType(String instanceType) {
		if ("Target Definition".equals(instanceType)) {
			return "TARGETINSTANCE";
		} else {
			return "TRANSFORMATIONINSTANCE";
		}
	}

	/**
	 * 添加依赖关系
	 * 
	 * @param fromBO
	 * @param toBO
	 * @param toType
	 */
	private void addDependency(PcBO fromBO, List<PcBO> toBOList, String toType) {
		for (int i = 0; i < toBOList.size(); i++) {
			PcBO toBO = toBOList.get(i);
			fromBO.addDependency(toBO.getId(), "target");
			if (!"Target Definition".equals(toType)) {
				toBO.addDependency(fromBO.getId(), "source");
			}
		}

	}
}
