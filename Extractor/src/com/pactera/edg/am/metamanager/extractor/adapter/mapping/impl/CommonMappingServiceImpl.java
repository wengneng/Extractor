/*
 * Copyright 2009 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, BeiJing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Element;
import org.dom4j.Node;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlAdapter;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.dao.IMetadataDao;
import com.pactera.edg.am.metamanager.extractor.ex.SpringContextLoadException;
import com.pactera.edg.am.metamanager.extractor.util.Dom4jReader;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;

/**
 * 公共目标xml转换为统一对象模型的实现类
 * 
 * @author user
 * @version 1.0 Date: Aug 24, 2009
 * 
 */
public class CommonMappingServiceImpl extends BaseMappingServiceImpl implements IMetadataMappingService {

	private Log log = LogFactory.getLog(CommonMappingServiceImpl.class);

	/**
	 * 采用XSLT采集数据源的公共接口
	 */
	private AbstractXmlAdapter commonAdapter;

	private IMetadataDao metadataDao;

	/**
	 * 缓存元数据在适配器中的唯一标识,与该元数据的一一对应
	 */
	private Map<Integer, MMMetadata> metadatasCache = new HashMap<Integer, MMMetadata>();

	/**
	 * 统一对象模型的实现类
	 * 
	 * @throws SpringContextLoadException
	 *             加载Spring配置异常
	 */
	public CommonMappingServiceImpl() throws SpringContextLoadException
	{
		metadataDao = (IMetadataDao) ExtractorContextLoader.getBean(IMetadataDao.SPRING_NAME);
	}

	private void parseSingleStream(InputStream iStream, AppendMetadata aMetadata) throws Exception {
		Dom4jReader reader = new Dom4jReader();

		try {
			if (reader.initDocument(iStream)) {
				try {
					iStream.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}

				// 悬挂结点下面的直接子结点列表
				List<Element> elements = reader.selectNodes("/root/instances/instance");
				if (elements != null && elements.size() > 0) {
					aMetadata.setChildMetaModels(genRootMetaModels(elements, aMetadata.getMetadata(), aMetadata
							.getMetaModel()));
					aMetadata.setDDependencies(genDependencies(reader.selectNodes("//relationships/relationship")));

				}
			}
		}
		finally {
			if (iStream != null) {
				try {
					iStream.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			reader.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService#metadataMapping()
	 */
	public void metadataMapping(AppendMetadata aMetadata) throws Exception {
		InputStream iStream = null;
		try {
			iStream = commonAdapter.parse();
			parseSingleStream(iStream, aMetadata);
		}
		finally {
			metadatasCache.clear();
		}
	}

	private List<MMDDependency> genDependencies(List<Element> elements) {

		/**
		 * 去除重复
		 */
		Set<String> duplicateRelationships = new HashSet<String>();

		List<MMDDependency> dependencies = new ArrayList<MMDDependency>();
		// 依赖关系
		for (Element element : elements) {
			MMDDependency dependency = new MMDDependency();
			// 由于XML文件中通过XSLT转换后，属性值包含空格等字符，因此需要trim()，add by fbchen 2011-01-12
			int ownerId = element.attributeValue("frominstanceid").trim().hashCode();
			int valueId = element.attributeValue("toinstanceid").trim().hashCode();
			if (metadatasCache.containsKey(ownerId) && metadatasCache.containsKey(valueId)) {
				// 将OWNERID与VALUEID拼凑起来,作为关系ID
				String relationshipId = new StringBuilder().append(ownerId).append("_").append(valueId).toString();

				if (duplicateRelationships.contains(relationshipId)) {
					log.warn(new StringBuilder("依赖关系重复:依赖端元数据:").append(metadatasCache.get(ownerId).getCode()).append(
							",被依赖端元数据:").append(metadatasCache.get(valueId).getCode()).toString());
					continue;
				}
				duplicateRelationships.add(relationshipId);

				dependency.setOwnerMetadata(metadatasCache.get(ownerId));
				dependency.setValueMetadata(metadatasCache.get(valueId));
				dependency.setOwnerRole(element.attributeValue("deprole"));
				dependency.setValueRole(element.attributeValue("depedrole"));
				dependencies.add(dependency);
			}
		}

		return dependencies;
	}

	/**
	 * 获取根元模型
	 * 
	 * @param elements
	 * @param parentMetadata
	 * @param parentMetaModel
	 * @return
	 */
	private Set<MMMetaModel> genRootMetaModels(List<Element> elements, MMMetadata parentMetadata,
			MMMetaModel parentMetaModel) {
		Set<MMMetaModel> metaModels = new HashSet<MMMetaModel>(2);

		// 将list反转排序,确保出现重复时,只取最后一个元素
		Collections.reverse(elements);
		/**
		 * 去除重复,考虑不同模型
		 */
		Set<String> duplicateElement = new HashSet<String>();
		for (Element element : elements) {

			String modelId = element.attributeValue("class");

			String code = element.selectSingleNode("./attributes/instancecode").getText();
			if (code == null || code.equals("")) {
				log.warn(new StringBuilder("Code为空!").append(",元模型:").append(modelId).toString());
				continue;
			}
			String duplicateString = new StringBuilder().append(code).append("_").append(modelId).toString();

			if (duplicateElement.contains(duplicateString)) {
				// 元模型相同,同时CODE相同,则认为重复
				log.warn(new StringBuilder("重复的Element:").append(code).append(",元模型:").append(modelId).toString());
				continue;
			}

			duplicateElement.add(duplicateString);

			MMMetadata relativeMetadata = parentMetadata;
			MMMetaModel relativeModel = parentMetaModel;
			// 只有Excel才有的相对路径
			Node pathNode = element.selectSingleNode("./path");

			if (pathNode != null) {
				relativeMetadata = genRelativeMetadata(parentMetadata, pathNode.getText());
				if (relativeMetadata == null) {
					continue;
				}
				relativeModel = new MMMetaModel();
				relativeModel.setCode(relativeMetadata.getClassifierId());
			}

			MMMetaModel cntMetaModel = getMetaModel(relativeModel, metaModels, element);
			MMMetadata metadata = genMetadata(relativeMetadata, cntMetaModel, element);
//			cntMetaModel.addMetadata(metadata);
//			cntMetaModel.setHasMetadata(true);
			List<Element> childElements = element.selectNodes("./instances/instance");
			if (childElements != null && childElements.size() > 0) {
				genMetaModels(childElements, metadata, cntMetaModel);
//				cntMetaModel.setHasChildMetaModel(true);
			}
		}
		return metaModels;
	}

	/**
	 * 只专门针对于Excel模板的采集，Excel除了有悬挂结点，还会有相对路径(指存在于悬挂结点之下,且已经存在于库表中的部分路径)
	 * 此时需作增量比较的数据结点已经不是悬挂结点,而是这些相对路径中的叶子结点(后续增量分析有用)
	 * 
	 * @param dsMetadataId
	 *            数据源悬挂结点的ID
	 * @param relativePath
	 *            相对路径,格式:/metadataCode(metaModelCode)/metadataCode(metaModelCode)/...
	 * @return
	 */
	private MMMetadata genRelativeMetadata(MMMetadata parentMetadata, String relativePath) {
		String[] relativePaths = relativePath.split("/");
		if (relativePaths.length <= 1) { return parentMetadata; }
		String dsMetadataId = parentMetadata.getId();
		String parentId = dsMetadataId;
		MMMetadata metadata = new MMMetadata();
		int index, lastIndex;
		String metadataCode, modelCode;

		for (int i = 1; i < relativePaths.length; i++) {
			// 从１开始，因为第０个数据为空，因为数据是以"/"开始的
			index = relativePaths[i].indexOf("(");
			lastIndex = relativePaths[i].indexOf(")");
			metadataCode = relativePaths[i].substring(0, index);
			modelCode = relativePaths[i].substring(index + 1, lastIndex);

			metadata.setCode(metadataCode);
			metadata.setClassifierId(modelCode);

			if (!metadataDao.existMetadata(parentId, metadata)) {
				// 库表中不存在该元数据，认为数据错误，
				log.error(new StringBuilder("相对路径:").append(relativePath).append(",库表中不存在父结点为:").append(parentId)
						.append(", 元数据code:").append(metadataCode).append(", 所属元模型:").append(modelCode).append(
								",的元数据,跳过处理.").toString());
				return null;
			}
			parentId = metadata.getId();

		}

		metadata.setHasExist(true);
		return metadata;
	}

	private MMMetaModel getMetaModel(MMMetaModel parentMetaModel, Set<MMMetaModel> metaModels, Element element) {
		boolean existMetaModel = false;
		MMMetaModel cntMetaModel = null;

		String metaModelCode = element.attributeValue("class");
		for (Iterator<MMMetaModel> iter = metaModels.iterator(); iter.hasNext();) {
			MMMetaModel metaModel = iter.next();
			if (metaModel.getCode().equals(metaModelCode)) {
				// 元模型已经存在,此时添加其元数据即可
				cntMetaModel = metaModel;
				existMetaModel = true;
				break;
			}

		}
		if (!existMetaModel) {
			cntMetaModel = new MMMetaModel();
			cntMetaModel.setCode(metaModelCode);
			cntMetaModel.setParentMetaModel(parentMetaModel);
			// String compedRelationCode =
			// element.attributeValue("composition");
			// if (compedRelationCode != null) {
			// cntMetaModel.setCompedRelationCode(compedRelationCode);
			// }
			metaModels.add(cntMetaModel);

		}
		return cntMetaModel;
	}

	private void genMetaModels(List<Element> elements, MMMetadata parentMetadata, MMMetaModel parentMetaModel) {

		// 将list反转排序,确保出现重复时,只取最后一个元素
		Collections.reverse(elements);
		/**
		 * 去除重复,CODE及模型同时相同的去除
		 */
		Set<String> duplicateElement = new HashSet<String>();
		for (Element element : elements) {
			String modelId = element.attributeValue("class");

			String code = element.selectSingleNode("./attributes/instancecode").getText();
			if (code == null || code.equals("")) {
				log.warn(new StringBuilder("Code为空!").append(",元模型:").append(modelId).toString());
				continue;
			}

			String duplicateString = new StringBuilder().append(code.hashCode()).append("_").append(modelId.hashCode())
					.toString();

			if (duplicateElement.contains(duplicateString)) {
				// 元模型相同,同时CODE相同,则认为重复
				log.warn(new StringBuilder("重复的Element:").append(code).append("，元模型:").append(modelId).toString());
				continue;
			}

			duplicateElement.add(duplicateString);

			MMMetaModel cntMetaModel = getMetaModel(parentMetaModel, element);
			MMMetadata metadata = genMetadata(parentMetadata, cntMetaModel, element);
//			cntMetaModel.addMetadata(metadata);
//			cntMetaModel.setHasMetadata(true);
			List<Element> childElements = element.selectNodes("./instances/instance");
			if (childElements != null && childElements.size() > 0) {
				genMetaModels(childElements, metadata, cntMetaModel);
//				cntMetaModel.setHasChildMetaModel(true);
			}
		}
	}

	private MMMetaModel getMetaModel(MMMetaModel parentMetaModel, Element element) {
		boolean existMetaModel = false;
		MMMetaModel cntMetaModel = null;
		String metaModelCode = element.attributeValue("class");

		for (Iterator<MMMetaModel> iter = parentMetaModel.getChildMetaModels().iterator(); iter.hasNext();) {
			MMMetaModel childMetaModel = iter.next();
			if (childMetaModel.getCode().equals(metaModelCode)) {
				// 元模型已经存在,此时添加其元数据即可
				cntMetaModel = childMetaModel;
				existMetaModel = true;
				break;
			}

		}
		if (!existMetaModel) {
			parentMetaModel.addChildMetaModels(metaModelCode);
			cntMetaModel = parentMetaModel.getChildMetaModel(metaModelCode);
		}
		return cntMetaModel;
	}

	private MMMetadata genMetadata(MMMetadata parentMetadata, MMMetaModel cntMetaModel, Element element) {
		new MMMetadata();
		// metadata.setClassifierId(element.attributeValue("class"));
		String code = element.selectSingleNode("./attributes/instancecode").getText();
		String name = element.selectSingleNode("./attributes/instancename").getText();
		// metadata.setCode(code == null ? "" : code.trim());
		// metadata.setName(name == null ? "" : name.trim());
		MMMetadata metadata = super.createMetadata(parentMetadata, cntMetaModel, code == null ? "" : code.trim(),
				name == null ? "" : name.trim());
		// metadata.setParentMetadata(parentMetadata);
		//		
		// parentMetadata.addChildMetadata(metadata);

		Map<String, String> attrs = new HashMap<String, String>();

		List<Element> attrElements = ((Element) element.selectSingleNode("./features")).elements();
		for (Element attrElement : attrElements) {
			String value = attrElement.getText();
			value = (value == null) ? null : value.trim(); // 由于XML标签中间会出现空格等字符，需要trim()
			if (value == null || value.equals("")) {
				continue;
			}
			attrs.put(attrElement.getName(), value);
		}
		metadata.setAttrs(attrs);

		// 缓存元数据
		metadatasCache.put(element.selectSingleNode("./attributes/instanceid").getText().hashCode(), metadata);
		return metadata;
	}

	public void setService(AbstractXmlAdapter service) {
		this.commonAdapter = service;
	}

}
