package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;

import com.pactera.edg.am.metamanager.app.metamodel.bs.IClassifierQueryBS;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsConfig;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateComp;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateDep;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateMdAttr;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleJoint;
import com.pactera.edg.am.metamanager.core.util.FormatUtil;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.TemplateMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Dom4jReader;

// import com.pactera.edg.am.metamanager.extractor.util.TemplateConfigReader;

public class ExcelTemplateMappingServiceImpl extends BaseMappingServiceImpl implements IMetadataMappingService {

	private Log log = LogFactory.getLog(ExcelTemplateMappingServiceImpl.class);

	private final String cellNodeName = "[name()='Cell']";

	private List<MMMetaModel> models = new ArrayList<MMMetaModel>();

	private int count = 0;

	private StringBuffer errMsg = new StringBuffer();

	private StringBuffer codeErrMsg = new StringBuffer();

	/**
	 * 缓存依赖关系
	 */
	private List<MMDDependency> dependencies = new ArrayList<MMDDependency>();

	private List<MetaTemplateDep> depcacheList = new ArrayList<MetaTemplateDep>();

	private int t_start;

	// 需转换成大写的变量;需转换成小写的变量
	// private boolean toUpperCase = true, toLowerCase = false;

	public void metadataMapping(AppendMetadata aMetadata) throws Exception {
		// 获取模板编号，取得模板配置
		IClassifierQueryBS iClassifier = AdapterExtractorContext.getInstance().getIClassifier();
		String dsId = AdapterExtractorContext.getInstance().getDatasourceId();
		List<TemplateClsConfig> tcc = iClassifier.getTemplateClsConfigsByDs(dsId);
		// String path_config = "D:/workspace/test2/templateConfig.xml";
		// TemplateConfigReader readerConfig = new TemplateConfigReader();
		// List<TemplateClsConfig> tcc1 = readerConfig.setupConfig(path_config);
		// List<TemplateClsConfig> tcc =
		// new ExcelTemplateMappingServiceImplTest().getTemplateConfigList();

		// 获取模板编号，取得模板配置结束
		String path = AdapterExtractorContext.getInstance().getDsAbsolutePath();

		try {

			log.debug(new StringBuilder("开始解析Excel的.xml文件,文件绝对路径:").append(path).append(" ,...").toString());
			Dom4jReader reader = new Dom4jReader();

			MMMetadata appendMd = aMetadata.getMetadata();
			MMMetaModel appendMm = aMetadata.getMetaModel();

			for (TemplateClsConfig template : tcc) {
				List<TemplateClsTitle> titleList1 = template.getClsTitles();
				reader.initDocument(new FileInputStream(new File(path)));
				Document doc = reader.getDocument();
				reader.close();
				MMMetaModel metaModel = getMapping(doc, appendMd, appendMm, titleList1, template.getClsid(), appendMm);
				reader.initDocument(new FileInputStream(new File(path)));
				doc = reader.getDocument();
				reader.close();
				parseComedClsConfig(doc, appendMd, appendMm, template, metaModel);
			}

			log.debug("---开始解析依赖关系---");
			// 如果是表级映射和字段级映射要增加设置依赖关系
			for (MetaTemplateDep metaDep : depcacheList) {
				List<TemplateDep> templateDeps = metaDep.getDepedList();
				MMMetadata depMd = metaDep.getDepMd();
				Node worksheetNode = metaDep.getCurrentWorksheet();
				int level = metaDep.getLevel();
				int row = metaDep.getRow();
				// 如果是depMd为空
				if (depMd == null) {
					List<TemplateComp> deps = metaDep.getDepList();
					depMd = getDependencydMeta(appendMm, appendMd, deps, worksheetNode, level, row, row);
					if (depMd == null) {
						depMd = this.createMd(appendMm, appendMd, deps, worksheetNode, level, row, row);
					}

					if (depMd == null) {
						continue;
					}
				}

				for (TemplateDep templateDep : templateDeps) {
					log.debug("依赖端代码 = " + depMd.getCode() + "---当前的worksheet = "
							+ metaDep.getCurrentWorksheet().selectSingleNode("@ss:Name").getStringValue() + "---"
							+ " row = " + metaDep.getRow() + " -- level = " + metaDep.getLevel());
					List<TemplateComp> comps = templateDep.getRelateConds();

					MMMetadata depedMd = getDependencydMeta(appendMm, null, comps, worksheetNode, level, row, row);
					if (depedMd == null) {
						depedMd = this.createMd(appendMm, appendMd, comps, worksheetNode, level, row, row);
					}
					if (depedMd != null) {
						addDepRela(depMd, templateDep.getFrole(), depedMd, templateDep.getTrole());
					}
				}
			}

			if (dependencies.size() > 0) {
				aMetadata.setDDependencies(dependencies);
			}

			aMetadata.setChildMetaModels(appendMm.getChildMetaModels());
			aMetadata.setMetaModel(appendMm);

			log.info("\n---共创建依赖关系 = " + dependencies.size() + "---");
			log.info("\n---共创建元数据的数据 = " + count + "---");
			log.info("文件解析完成!");

			// 输出未创建成功的元数据信息到日志模块
			String[] errs = errMsg.toString().split("////");
			for (String err : errs) {
				if (err.length() > 0) {
					AdapterExtractorContext.addExtractorLog(ExtractorLogLevel.WARN, err);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			log.debug("Excel XML文件解析失败, 文件绝对路径:" + path, e);
		} finally {
			// 作数据清理---一定要做数据清理,问题是,如何通过策略来保证?采用模板模式?
			count = 0;
			dependencies = new ArrayList<MMDDependency>();
			depcacheList = new ArrayList<MetaTemplateDep>();
			t_start = 0;
			codeErrMsg = new StringBuffer();
			errMsg = new StringBuffer();
			// 模型应该一致的,可以不用清空
			// models = null;

		}
	}

	private MMMetadata createMd(MMMetaModel appendMm, MMMetadata appendMd, List<TemplateComp> compedList,
			Node relationSheetNode, int level, int row, int seq) throws Exception {
		MMMetaModel metamodel = appendMm;
		MMMetadata metadata = appendMd;
		for (int j = 0; j < compedList.size(); j++) {
			String worksheetName = relationSheetNode.selectSingleNode("@ss:Name").getStringValue();
			TemplateComp comp = compedList.get(j);

			MMMetaModel annModel = getModel(models, comp.getCompClsId(), metamodel);
			annModel.setHaveChangelessAttr(true);
			TemplateMetadata aMd = new TemplateMetadata();
			aMd.setClassifierId(annModel.getCode());
			aMd.setParentMetadata(metadata);

			List<TemplateTitleJoint> templateTitleJoints = comp.getTitleJnts().getTitleJoints();
			StringBuffer comlumCodeBuff = new StringBuffer();
			for (TemplateTitleJoint templateTitleJoint : templateTitleJoints) {
				comlumCodeBuff.append(getRelationCloumNum(relationSheetNode, templateTitleJoint, level, row)).append(
						"-");
			}

			String metadataCode = comlumCodeBuff.substring(0, comlumCodeBuff.length() - 1);
			// 设置元数据的代码
			if (!(metadataCode != null && metadataCode.length() > 0)) {
				log.debug("---- 元数据的代码为空，元数据创建失败！  ----");
				return null;
			}
			if (isBlank(metadataCode)) {
				log.debug("---- code中仅包含控制字符 -----");
				return null;
			}

			metadataCode = metadataCode.trim().toUpperCase();
			aMd.setCode(metadataCode);

			// 如果code的长度大于200则不入库
			if (aMd.getCode().length() > 200) {
				codeErrMsg.append("\n--- 元数据的代码的长度大于200[当前的worksheet=" + worksheetName + "---当前行=" + seq + "---当前模型="
						+ annModel.getCode() + "]");
				log.debug("----code的长度大于200 code = ------" + aMd.getCode());
				return null;
			}

			MMMetadata findMeta = this.findExist(annModel.getMetadatas(), aMd, metadata);
			if (findMeta == null) {
				// 如果元数据不存在则创建
				// 设置元数据的名称
				aMd.setName(metadataCode);
				aMd.setParentMetadata(metadata);
				if (metadata instanceof TemplateMetadata) {
					((TemplateMetadata) metadata).addChild(aMd);
				}
				// 将元数据放入元模型中
				annModel.addMetadata(aMd);
				annModel.setHasMetadata(true);
				count++;
			} else {
				// 如果元数据存在，则用已存在的元数据
				aMd = (TemplateMetadata) findMeta;
			}

			metamodel = annModel;
			metadata = aMd;
		}

		return metadata;
	}

	/**
	 * 判断当前配置模板对象是否有下一级对象，如果有迭代解析其下级配置模板对象
	 * 
	 * @param document
	 *            xml文档
	 * @param appendMd
	 *            挂靠元数据
	 * @param template
	 *            当前配置模板对象
	 * @param appendModel
	 *            挂靠元模型
	 * @throws Exception
	 */
	private void parseComedClsConfig(Document document, MMMetadata appendMd, MMMetaModel appendMm,
			TemplateClsConfig template, MMMetaModel appendModel) throws Exception {
		List<TemplateClsConfig> compedList = template.getCompeds();
		// 如果上一级模型中未创建任何元数据，则其下一层不再遍历
		if (appendModel.getMetadatas().size() > 0 && compedList != null && compedList.size() > 0) {
			for (TemplateClsConfig comped : compedList) {
				List<TemplateClsTitle> compedTitleList1 = comped.getClsTitles();
				MMMetaModel metaModel = getMapping(document, appendMd, appendMm, compedTitleList1, comped.getClsid(),
						appendModel);
				parseComedClsConfig(document, appendMd, appendMm, comped, metaModel);
			}
		} else {
			return;
		}
	}

	/**
	 * 解析xml文档，根据modelType创建当前元模型，以及属于当前元模型的所有元数据，并设置元模型的关系
	 * 元数据的组合关系、依赖关系，最后返回当前元模型(作为下一级元模型的挂靠元模型)
	 * 
	 * @param document
	 *            xml文档
	 * @param appendMd
	 *            挂靠的元数据
	 * @param titleList
	 *            当挂靠元模型下的所有需要创建的元数据的标题对象列表
	 * @param modelType
	 *            元模型的类型代码例如"Table","TableMapping"
	 * @param metaModel
	 *            挂靠的元模型
	 * @return
	 * @throws Exception
	 */
	private MMMetaModel getMapping(Document document, MMMetadata appendMd, MMMetaModel appendMm,
			List<TemplateClsTitle> titleList, String modelType, MMMetaModel metaModel) throws Exception {
		// 获得元模型
		MMMetaModel annModel = getModel(models, modelType, metaModel);
		for (TemplateClsTitle title : titleList) {
			int start = title.getDstart();
			int titleEnd = title.getTend().intValue();
			int titleStart = title.getTstart().intValue();
			int level = titleEnd - titleStart + 1;
			String endFlag = title.getDendtext();

			t_start = titleStart;
			// 获取标题的结束行的下一行，和数据的开始行之间的行数
			// 标题的结束行和数据的开始行之间有其他的数据行relativePos>0
			// 如果标题结束行的下一行就是数据的开始行，那么relativePos=0
			int relativePos = start - (titleEnd + 1);
			log.debug(" -- " + annModel.getCode() + " --- start --");

			List<Element> worksheetNodeList = parseTitleColumn(document, title.getMdCode().getTitleJoints().get(0),
					level);

			for (Node worksheetNode : worksheetNodeList) {
				// 验证当前sheet页面上是否包含所需要解析的所有字段
				if (!validateSheet(worksheetNode, title, level)) {
					log.debug(" -----当前sheet页面 \"" + worksheetNode.selectSingleNode("@ss:Name").getStringValue()
							+ "\" 不包含要解析的标题， 丢弃该sheet页面 ---- ");
					continue;
				}

				// 如果没设置数据的开始行，则默认取标题的下一行为数据的开始行
				start = getDataStartIndex(worksheetNode, title);
				if (relativePos > 0) {
					start = start + relativePos;
				}

				int end = getDataEndIndex(worksheetNode, endFlag);
				String worksheetName = worksheetNode.selectSingleNode("@ss:Name").getStringValue();
				log.debug(" -- worksheeet name = " + worksheetNode.selectSingleNode("@ss:Name").getStringValue()
						+ " -- start row = " + start + " -- end row = " + end);
				// 克隆worksheetNode
				String text = worksheetNode.asXML();
				Document tempDoc = DocumentHelper.parseText(text);
				Node tempEle = tempDoc.selectSingleNode("*[name()='Worksheet']");
				for (int i = start; i <= end; i++) {

					// 如果元数据的名称和属性都没配置，那么没有配置实体，只配置关系
					// if (title.getMdName() == null && title.getMdAttrs() ==
					// null) {
					if (!(title.getMdAttrs() != null && title.getMdAttrs().size() > 0) && title.getMdName() == null) {
						cacheDependency(annModel, appendMd, appendMm, annModel.getCode(), title, worksheetNode,
								tempEle, level, start, i);
					} else {
						MMMetadata composingMd = parseMd(worksheetNode, tempEle, worksheetName, annModel, appendMd,
								appendMm, start, i, title, level);
						log.debug("元模型代码" + composingMd.getCode());
					}
				}
			}

			log.debug(" -- end -- ");
		}

		return annModel;
	}

	/**
	 * 验证当前worksheet是否包含所有的列的标题
	 * 
	 * @param worksheetNode
	 *            当前的worksheet
	 * @param relationStr
	 *            需要验证的列的标题
	 * @return true 包含所有的，false，不包含所有的
	 */
	private boolean validateSheet(Node worksheetNode, TemplateClsTitle curTitle, int level) {
		boolean isUnique = true;
		// 判断代码是否存在
		List<TemplateTitleJoint> codeTitles = curTitle.getMdCode().getTitleJoints();
		for (TemplateTitleJoint codeTitle : codeTitles) {
			if (getColumnIndex(worksheetNode, codeTitle, level) == null) {
				isUnique = false;
				break;
			}
		}

		if (curTitle.getMdName() != null) {
			List<TemplateTitleJoint> nameTitles = curTitle.getMdName().getTitleJoints();
			// 判断名称是否存在
			for (TemplateTitleJoint nameTitle : nameTitles) {
				if (getColumnIndex(worksheetNode, nameTitle, level) == null) {
					isUnique = false;
					break;
				}
			}
		}

		// 判断组合关系是否存在
		List<TemplateComp> comps = curTitle.getComps();
		for (TemplateComp comp : comps) {
			List<TemplateTitleJoint> compedTitles = comp.getTitleJnts().getTitleJoints();
			for (TemplateTitleJoint compedTitle : compedTitles) {
				if (getColumnIndex(worksheetNode, compedTitle, level) == null) {
					isUnique = false;
					break;
				}
			}
		}

		// 判断依赖关系是否存在
		List<TemplateDep> deps = curTitle.getDeps();
		for (TemplateDep dep : deps) {
			List<TemplateComp> compedTitles = dep.getRelateConds();
			for (TemplateComp compedTitle : compedTitles) {
				List<TemplateTitleJoint> depTitles = compedTitle.getTitleJnts().getTitleJoints();
				for (TemplateTitleJoint depTitle : depTitles) {
					if (getColumnIndex(worksheetNode, depTitle, level) == null) {
						isUnique = false;
						break;
					}
				}
			}
		}

		// 判断属性是否存在
		List<TemplateMdAttr> attrs = curTitle.getMdAttrs();
		for (TemplateMdAttr attr : attrs) {
			List<TemplateTitleJoint> attrTitles = attr.getTitleJnts().getTitleJoints();
			for (TemplateTitleJoint attrTitle : attrTitles) {
				if (getColumnIndex(worksheetNode, attrTitle, level) == null) {
					isUnique = false;
					break;
				}
			}
		}

		return isUnique;
	}

	/**
	 * 设置元数据的依赖关系
	 * 
	 * @param depMd
	 *            依赖端元数据
	 * @param depRole
	 *            依赖端角色
	 * @param depedMd
	 *            被依赖元数据
	 * @param depedRole
	 *            被依赖端角色
	 */
	private void addDepRela(MMMetadata depMd, String depRole, MMMetadata depedMd, String depedRole) {
		if (isDependencyExists(depMd, depRole, depedMd, depedRole)) {
			return;
		}
		MMDDependency mmd = new MMDDependency();
		mmd.setOwnerMetadata(depMd);// 依赖端
		mmd.setOwnerRole(depRole);
		mmd.setValueMetadata(depedMd);// 被依赖端
		mmd.setValueRole(depedRole);
		dependencies.add(mmd);
	}

	/**
	 * 判断依赖关系是否存在
	 * 
	 * @param depMd
	 * @param depRole
	 * @param depedMd
	 * @param depedRole
	 * @return
	 */
	private boolean isDependencyExists(MMMetadata depMd, String depRole, MMMetadata depedMd, String depedRole) {
		boolean isExists = false;
		for (MMDDependency dep : dependencies) {
			if (dep.getOwnerRole() == depRole && isEquals(dep.getOwnerMetadata(), depMd)
					&& dep.getValueRole() == depedRole && isEquals(dep.getValueMetadata(), depedMd)) {
				isExists = true;
				codeErrMsg.append("\n--- 依赖关系已存在[当前的OwnerMetadata =" + depMd.getCode()
						+ "---OwnerMetadata classifierId=" + depMd.getClassifierId() + "---ValueMetadata ="
						+ depedMd.getCode() + " ---ValueMetadata classifierId=" + depedMd.getClassifierId() + "]");
				log.debug("OwnerMetadata = " + depMd.getCode() + " OwnerMetadata classifierId="
						+ depMd.getClassifierId() + " ValueMetadata = " + depedMd.getCode()
						+ " ValueMetadata classifierId=" + depedMd.getClassifierId());
			}
		}
		return isExists;
	}

	/**
	 * 根据关系的列明和所在的行数获取关联代码。
	 * 
	 * @param relationSheetNode
	 *            当前的worksheet
	 * @param relationColumn
	 *            关系的列
	 * @param level
	 *            标题的级别
	 * @param row
	 *            关联元数据所在的行数
	 * @return
	 * @throws Exception
	 */
	private String getRelationCloumNum(Node relationSheetNode, TemplateTitleJoint relationColumn, int level, int row)
			throws Exception

	{
		String relationColumNum = getColumnIndex(relationSheetNode, relationColumn, level);
		int relationNumber = Integer.valueOf(relationColumNum).intValue();
		Node dataNode = relationSheetNode.selectSingleNode("*[name()='Table']/*[name()='Row'][" + row + "]");
		// 取得关联列的代码
		Node relationNode = getColumnNodeByNum(dataNode, String.valueOf(relationNumber));
		String relationName = "";
		if (relationNode != null && relationNode.getStringValue().length() > 0) {
			relationName = relationNode.getStringValue();
		}

		return relationName;
	}

	private MMMetadata parseDepMd(Node worksheetNode, MMMetaModel tableModel, MMMetadata appendMd,
			MMMetaModel appendMm, int row, int seq, TemplateClsTitle templateClsTitle, int level) throws Exception {

		// 创建元数据
		TemplateMetadata aMd = new TemplateMetadata();
		aMd.setClassifierId(tableModel.getCode());
		aMd.setParentMetadata(appendMd);

		List<TemplateTitleJoint> codeTitleJoints = templateClsTitle.getMdCode().getTitleJoints();
		Node dataNode = worksheetNode.selectSingleNode("*[name()='Table']/*[name()='Row'][" + row + "]");

		String worksheetName = worksheetNode.selectSingleNode("@ss:Name").getStringValue();
		// 设置元数据的代码
		List<String> codeList = new ArrayList<String>();
		for (TemplateTitleJoint codeTitleJoint : codeTitleJoints) {
			// 找到元数据所在的列数
			String columNum = getColumnIndex(worksheetNode, codeTitleJoint, level);
			Node codeNode = getColumnNodeByNum(dataNode, columNum);

			if (codeNode == null) {
				log.debug(" ---- 元数据的代码为空，元数据创建失败！  ----");
				return aMd;
			}
			String code = codeNode.getStringValue();
			if (!(code != null && code.length() > 0)) {
				log.debug("---- 元数据的代码为空，元数据创建失败！  ----");
				return aMd;
			}
			if (isBlank(code)) {
				log.debug("---- code中仅包含控制字符 -----");
				return aMd;
			}
			// trim并且转换成大写
			code = code.trim().toUpperCase();
			codeList.add(code);
			String codeValue = aMd.getCode();
			aMd.setCode((codeValue != null && codeValue.length() > 0) ? codeValue + "-" + code : code);
		}

		// 如果code的长度大于200则不入库
		if (aMd.getCode().length() > 200) {
			codeErrMsg.append("\n--- 元数据的代码的长度大于200[当前的worksheet=" + worksheetName + "---当前行=" + seq + "---当前模型="
					+ tableModel.getCode() + "]");
			log.debug("----code的长度大于200 code = ------" + aMd.getCode());
			return aMd;
		}

		// 找父节点，并且判断当前元数据是否重复
		List<TemplateComp> compedList = templateClsTitle.getComps();
		if (compedList.size() > 0) {
			appendMd = getDependencydMeta(appendMm, appendMd, compedList, worksheetNode, level, row, seq);
			if (appendMd == null) {
				errMsg.append("---创建关系中的元数据代码=" + aMd.getCode() + "---当前模型=" + tableModel.getCode() + "]");
				log.debug("未找到父节点或祖先节点");
				return aMd;
			}
		}

		if (this.isExist(tableModel.getMetadatas(), aMd, appendMd)) {
			log.debug(" -- 元数据已存在，丢弃 code = " + aMd.getCode() + " -- name = " + aMd.getName());
			return aMd;
		}

		// 创建关系中的实体，名称与代码相同
		aMd.setName(aMd.getCode());

		// 设置组合关系，父子的组合
		aMd.setParentMetadata(appendMd);
		if (appendMd instanceof TemplateMetadata) {
			((TemplateMetadata) appendMd).addChild(aMd);
		}

		// 将元数据放入元模型中
		tableModel.addMetadata(aMd);
		// 设置是否不需要更新，这是关系中的实体因此不需要更新属性
		tableModel.setHaveChangelessAttr(true);
		tableModel.setHasMetadata(true);

		log.debug(" -- 元数据创建成功 -- code = " + aMd.getCode() + " -- name = " + aMd.getName());
		count++;
		return aMd;
	}

	/**
	 * 解析当前sheet获得所需的元数据以及组合关系
	 * 
	 * @param worksheetNode
	 *            当前的worksheet
	 * @param tableModel
	 *            当前的元模型
	 * @param appendMd
	 *            挂靠的元数据
	 * @param row
	 *            元数据所在的行
	 * @param column
	 *            列标题
	 * @param level
	 *            列标题的级别
	 * @param relationType
	 *            是在模型的开始节点还是结束节点System/Table/Column System
	 *            为开始节点，Table为中间节点，Column为结束节点。
	 * @param relationColumn
	 *            关联节点
	 * @return
	 * @throws Exception
	 */
	private MMMetadata parseMd(Node oriWorksheetNode, Node worksheetNode, String worksheetName, MMMetaModel tableModel,
			MMMetadata appendMd, MMMetaModel appendMm, int row, int seq, TemplateClsTitle templateClsTitle, int level)
			throws Exception {
		// 创建元数据
		TemplateMetadata aMd = new TemplateMetadata();
		aMd.setClassifierId(tableModel.getCode());
		aMd.setParentMetadata(appendMd);

		List<TemplateTitleJoint> codeTitleJoints = templateClsTitle.getMdCode().getTitleJoints();
		List<TemplateTitleJoint> nameTitleJoints = templateClsTitle.getMdName().getTitleJoints();
		Node dataNode = worksheetNode.selectSingleNode("*[name()='Table']/*[name()='Row'][" + row + "]");

		// 设置元数据的代码
		List<String> codeList = new ArrayList<String>();
		for (TemplateTitleJoint codeTitleJoint : codeTitleJoints) {
			// 找到元数据所在的列数
			String columNum = getColumnIndex(worksheetNode, codeTitleJoint, level);
			Node codeNode = getColumnNodeByNum(dataNode, columNum);

			if (codeNode == null) {
				log.debug(" ---- 元数据的代码为空，元数据创建失败！  ----");
				dataNode.detach();
				return aMd;
			}
			String code = codeNode.getStringValue();
			if (!(code != null && code.length() > 0)) {
				log.debug("---- 元数据的代码为空，元数据创建失败！  ----");
				dataNode.detach();
				return aMd;
			}
			if (isBlank(code)) {
				log.debug("---- code中仅包含控制字符 -----");
				dataNode.detach();
				return aMd;
			}
			// trim并且转换成大写
			code = code.trim().toUpperCase();
			codeList.add(code);
			String codeValue = aMd.getCode();
			aMd.setCode((codeValue != null && codeValue.length() > 0) ? codeValue + "-" + code : code);
		}

		// 如果code的长度大于200则不入库
		if (aMd.getCode().length() > 200) {
			codeErrMsg.append("\n--- 元数据的代码的长度大于200[当前的worksheet=" + worksheetName + "---当前行=" + seq + "---当前模型="
					+ tableModel.getCode() + "]");
			log.debug("----code的长度大于200 code = ------" + aMd.getCode());
			dataNode.detach();
			return aMd;
		}

		// 找父节点，并且判断当前元数据是否重复
		List<TemplateComp> compedList = templateClsTitle.getComps();
		if (compedList.size() > 0) {
			appendMd = getDependencydMeta(appendMm, appendMd, compedList, worksheetNode, level, row, seq);
			if (appendMd == null) {
				errMsg.append("---当前元数据代码=" + aMd.getCode() + "---当前模型=" + tableModel.getCode() + "]");
				log.debug("未找到父节点或祖先节点");
				dataNode.detach();
				return aMd;
			}
		}

		if (this.isExist(tableModel.getMetadatas(), aMd, appendMd)) {
			log.debug(" -- 元数据已存在，丢弃 code = " + aMd.getCode() + " -- name = " + aMd.getName());
			dataNode.detach();
			return aMd;
		}

		// 设置元数据的名称
		int index = 0;
		for (TemplateTitleJoint nameTitleJoint : nameTitleJoints) {
			// 找到元数据所在的列数
			String columNum = getColumnIndex(worksheetNode, nameTitleJoint, level);
			Node nameNode = getColumnNodeByNum(dataNode, columNum);
			String name = "";
			if (nameNode == null) {
				name = codeList.get(index);
			} else {
				name = nameNode.getStringValue();
			}

			if (!(name != null && name.length() > 0)) {
				name = codeList.get(index);
			}
			index++;
			String nameValue = aMd.getName();
			aMd.setName((nameValue != null && nameValue.length() > 0) ? nameValue + "-" + name : name);
		}

		// 设置元数据的属性
		List<TemplateMdAttr> attrMds = templateClsTitle.getMdAttrs();
		// 设置需要更新的属性
		if (templateClsTitle.isHaveChangelessAttr()) {
			tableModel.setHaveChangelessAttr(true);
			Set<String> attrNames = templateClsTitle.getNeedUpdateAttrs();
			for (String attrName : attrNames) {
				tableModel.addNeedUpdateAttr(attrName);
			}
		}

		for (TemplateMdAttr attrMd : attrMds) {
			List<TemplateTitleJoint> attrTitleJoints = attrMd.getTitleJnts().getTitleJoints();
			String attrCode = attrMd.getAttCode();
			for (TemplateTitleJoint attrTitleJoint : attrTitleJoints) {
				// 找到元数据所在的列数
				String columNum = getColumnIndex(worksheetNode, attrTitleJoint, level);
				Node attrNode = getColumnNodeByNum(dataNode, columNum);

				String attr = "";
				if (attrNode != null) {
					attr = attrNode.getStringValue();
					Node dataTypeNode = attrNode.selectSingleNode("*[name()='Data']");
					if (dataTypeNode != null) {
						attr = nodeValueConvert(dataTypeNode, attr);
					}
				}

				String attrValue = aMd.getAttr(attrCode);
				aMd.addAttr(attrCode, attrValue != null && attrValue.length() > 0 ? attrValue + "-" + attr : attr);
			}
		}

		// 设置组合关系，父子的组合
		aMd.setParentMetadata(appendMd);
		if (appendMd instanceof TemplateMetadata) {
			((TemplateMetadata) appendMd).addChild(aMd);
		}

		// 设置表级或字段级映射的依赖关系
		// column[0]表示表级映射或字段级映射的代码，而代码的第一个表示目标表，代码的第二位开始表示源表
		// 源表有1个，目标表可以为多个
		List<TemplateDep> templateDeps = templateClsTitle.getDeps();
		if (templateDeps != null && templateDeps.size() > 0) {
			MetaTemplateDep metaDep = new MetaTemplateDep();
			metaDep.setDepMd(aMd);
			metaDep.setDepedList(templateDeps);
			metaDep.setCurrentWorksheet(oriWorksheetNode);
			metaDep.setLevel(level);
			metaDep.setRow(seq);
			depcacheList.add(metaDep);
		}

		// 将元数据放入元模型中
		tableModel.addMetadata(aMd);
		tableModel.setHasMetadata(true);

		log.debug(" -- 元数据创建成功 -- code = " + aMd.getCode() + " -- name = " + aMd.getName());
		count++;
		dataNode.detach();
		return aMd;
	}

	private String nodeValueConvert(Node dataTypeNode, String attrValue) {
		String dataType = dataTypeNode.selectSingleNode("@ss:Type").getStringValue();

		if (dataType.equals("DateTime")) {
			return FormatUtil.formatDate(FormatUtil.parseTimestamp(attrValue.replaceAll("T", " ")), null);
		}

		return attrValue;
	}

	private void cacheDependency(MMMetaModel curAppendMm, MMMetadata appendMd, MMMetaModel rootAppendMm,
			String depClsId, TemplateClsTitle templateClsTitle, Node oriWorksheetNode, Node worksheetNode, int level,
			int row, int seq) throws Exception {
		List<TemplateDep> templateDeps = templateClsTitle.getDeps();
		TemplateComp comp = new TemplateComp();
		comp.setCompClsId(depClsId);
		List<TemplateComp> depList = new ArrayList<TemplateComp>();
		depList.addAll(templateClsTitle.getComps());
		depList.add(comp);
		comp.setTitleJnts(templateClsTitle.getMdCode());
		if (templateDeps != null && templateDeps.size() > 0) {
			MetaTemplateDep metaDep = new MetaTemplateDep();
			metaDep.setDepList(depList);
			metaDep.setDepedList(templateDeps);
			metaDep.setCurrentWorksheet(oriWorksheetNode);
			metaDep.setLevel(level);
			metaDep.setRow(seq);
			depcacheList.add(metaDep);
		} else {
			// 如果对于这类没有配置依赖关系，说明这个是关系的上一级创建元数据
			// 如果没有配置依赖关系，则创建关系中的实体
			parseDepMd(worksheetNode, curAppendMm, appendMd, rootAppendMm, row, seq, templateClsTitle, level);
		}
	}

	/**
	 * 根据当前行节点，获取给定列数的cell节点
	 * 
	 * @param rowNode
	 *            当前行节点
	 * @param columNum
	 *            列数
	 * @return 列数对应的列节点
	 */
	private Node getColumnNodeByNum(Node rowNode, String columNum) {
		List<Node> cellNodeList = rowNode.selectNodes("*[name()='Cell']");
		int columnIndex = 0;
		Node aCellNode = null;
		for (Node cellNode : cellNodeList) {
			Node indexAttr = cellNode.selectSingleNode("@ss:Index");
			if (indexAttr != null) {
				columnIndex = Integer.valueOf(indexAttr.getStringValue()).intValue();
			} else {
				columnIndex++;
			}

			if (String.valueOf(columnIndex).equals(columNum)) {
				aCellNode = cellNode;
				break;
			}

			if (columNum.equals("-1")) {
				aCellNode = cellNode;
				break;
			}
		}
		return aCellNode;
	}

	/**
	 * 根据关联字段列表获取关联的元数据（组合关系元数据，关联关系元数据）
	 * 
	 * @param appendMd
	 *            挂靠元数据
	 * @param relationStr
	 *            关联字段列表
	 * @param relationSheetNode
	 *            当前所在sheet
	 * @param level
	 *            标题的级别
	 * @param row
	 *            所在行数
	 * @param relationType
	 * @return 如果返回appendMd，表示没找到关联的，返回输入的元数据appendMd
	 * @throws Exception
	 */
	private MMMetadata getDependencydMeta(MMMetaModel appendMm, MMMetadata appendMd, List<TemplateComp> compedList,
			Node relationSheetNode, int level, int row, int seq) throws Exception {
		MMMetadata metaData = appendMd;
		MMMetaModel tempModel = appendMm;
		for (int j = 0; j < compedList.size(); j++) {
			boolean isFound = false;
			TemplateComp comp = compedList.get(j);
			List<TemplateTitleJoint> templateTitleJoints = comp.getTitleJnts().getTitleJoints();
			StringBuffer comlumCodeBuff = new StringBuffer();
			for (TemplateTitleJoint templateTitleJoint : templateTitleJoints) {
				comlumCodeBuff.append(getRelationCloumNum(relationSheetNode, templateTitleJoint, level, row)).append(
						"-");
			}
			String metadataCode = comlumCodeBuff.substring(0, comlumCodeBuff.length() - 1);
			if (metadataCode != null && metadataCode.length() > 0) {
				metadataCode = metadataCode.trim().toUpperCase();
			}
			tempModel = tempModel.getChildMetaModel(comp.getCompClsId());
			// 这类元数据不存在Excel中，需要创建新的元数据
			if (tempModel == null) {
				return null;
			}
			List metaList = new ArrayList();
			if (metaData instanceof TemplateMetadata) {
				metaList = ((TemplateMetadata) metaData).getChildren();
			} else {
				metaList = tempModel.getMetadatas();
			}
			for (int k = 0; k < metaList.size(); k++) {
				TemplateMetadata meta = (TemplateMetadata) metaList.get(k);
				if (metadataCode.equals(meta.getCode())) {
					metaData = (TemplateMetadata) meta;
					isFound = true;
				}
			}

			if (isFound) {
				continue;
			} else {
				if (appendMd != null) {
					errMsg.append("////---未找到父节点或祖先节点[当前的worksheet="
							+ relationSheetNode.selectSingleNode("@ss:Name").getStringValue() + "---当前行=" + seq
							+ "---未找到的父节点或祖先节点的模型=" + tempModel.getCode());
				}
				return null;
			}
		}

		return metaData;
	}

	/**
	 * 在给定sheet页面上获得给定标题所在的列
	 * 
	 * @param worksheetNode
	 *            当前的worksheet
	 * @param titleJoint
	 *            标题
	 * @param level
	 *            标题的级别
	 * @return
	 */
	private String getColumnIndex(Node worksheetNode, TemplateTitleJoint titleJoint, int level) {
		String startStr = "1,1";
		List<TemplateTitle> titles = titleJoint.getTitles();

		for (int i = t_start, j = 1; i < level + t_start; i++, j++) {
			TemplateTitle title = titles.get(j - 1);
			Integer mergeDownNum = title.getMergeDown();
			startStr = getCellNodeIndex(worksheetNode, i, startStr, title.getName(), level, mergeDownNum);

			if (startStr == null) {
				return null;
			}

			if (mergeDownNum != null && mergeDownNum.intValue() > 0) {
				i = i + 1 + mergeDownNum.intValue();
			}
		}

		return startStr;
	}

	/**
	 * 获得Cell所在的起止列数
	 * 
	 * @param worksheetNode
	 *            当前的worksheet
	 * @param row
	 *            行数
	 * @param startColum
	 *            cell的开始起止列数
	 * @param title
	 *            标题名称
	 * @param level
	 *            标题级别
	 * @param mergeDown
	 *            标题向下合并数
	 * @return
	 */
	private String getCellNodeIndex(Node worksheetNode, int row, String startColum, String title, int level,
			Integer mergeDown) {
		int start = Integer.valueOf(startColum.split(",")[0]).intValue();
		int end = Integer.valueOf(startColum.split(",")[1]).intValue();
		Node cellNode1 = null;
		if (end == 1) {
			cellNode1 = worksheetNode.selectSingleNode("*[name()='Table']/" + "*[name()='Row'][" + row + "]"
					+ "/*[name()='Cell'][position() >=" + start + "]/*[contains(name(),'Data')][text()=\'" + title
					+ "\']/parent::*");
		} else {
			cellNode1 = worksheetNode.selectSingleNode("*[name()='Table']/" + "*[name()='Row'][" + row + "]"
					+ "/*[name()='Cell'][position() >=" + start + " and position()<= " + +end + "]"
					+ "/*[contains(name(),'Data')][text()=\'" + title + "\']/parent::*");
		}

		if (cellNode1 == null) {
			log.debug("没有找到标题所在的列");
			return null;
		}

		List<Node> precedNode = cellNode1.selectNodes("preceding-sibling::*");
		Node mergeAccrossAttrNode = cellNode1.selectSingleNode("@ss:MergeAcross");
		Node mergeDownAttrNode = cellNode1.selectSingleNode("@ss:MergeDown");

		// 判断该列标题是否存在的标准，要看Cell所包含的值以及其MergeDown和MergeAcross属性是否都完全相同
		if (mergeDown != null && mergeDown.intValue() > 0) {
			if (mergeDownAttrNode == null) {
				log.debug("没找到标题所在的列");
				return null;
			} else if (!mergeDownAttrNode.getStringValue().equals(mergeDown.toString())) {
				log.debug("没找到标题所在的列");
				return null;
			}
		} else {
			if (mergeDownAttrNode != null) {
				log.debug("没找到标题所在的列");
				return null;
			}
		}

		// CellNode的唯一路径
		String cellNodePath = cellNode1.getUniquePath();

		for (Node preNode : precedNode) {
			Node mergeAttr = preNode.selectSingleNode("@ss:MergeAcross");
			if (mergeAttr == null) {
				start = start + 1;
			} else {
				start = start + 1 + Integer.valueOf(mergeAttr.getStringValue()).intValue();
			}
		}

		if (mergeDownAttrNode != null
				&& Integer.valueOf(mergeDownAttrNode.getStringValue()).intValue() + (row - t_start + 1) == level) {
			return String.valueOf(start);
		}

		if (mergeAccrossAttrNode == null && ((row - t_start + 1) == level)) {
			return String.valueOf(getCellNumByUniquePath(cellNodePath));
		} else if (mergeAccrossAttrNode == null) {
			end = start + 1;
		} else {
			end = start + 1 + Integer.valueOf(mergeAccrossAttrNode.getStringValue()).intValue();
		}

		return String.valueOf(start) + "," + String.valueOf(end);
	}

	/**
	 * 解析节点的唯一路径，取得cell的位置,获得cell所在的列
	 * 
	 * @param uniquePath
	 *            节点的唯一路径
	 * @return cell的位置
	 */
	private int getCellNumByUniquePath(String uniquePath) {
		int index = uniquePath.indexOf(cellNodeName) + cellNodeName.length();
		String columNum = "";

		if (uniquePath.length() == index) {
			return -1;
		}

		char aChar = uniquePath.charAt(index + 1);
		while (aChar != ']') {
			index++;
			columNum += aChar;
			aChar = uniquePath.charAt(index + 1);
		}

		return Integer.valueOf(columNum).intValue();
	}

	/**
	 * 根据模型代码以及父模型创建新的模型。
	 * 
	 * @param models
	 *            List 当前挂靠点下的模型列表
	 * @param modelCode
	 *            模型的代码
	 * @param parentMm
	 *            当前模型的父模型
	 * @return 返回根据模型代码创建的模型
	 */
	private MMMetaModel getModel(List<MMMetaModel> models, String modelCode, MMMetaModel parentMm) {

		MMMetaModel model = new MMMetaModel();

		model.setCode(modelCode);

		if (models.contains(model)) {
			int indexOf = models.indexOf(model);
			model = (MMMetaModel) models.get(indexOf);
		} else {
			models.add(model);
			model.setParentMetaModel(parentMm);
		}

		parentMm.addMetaModel(model);
		if (parentMm.isHasChildMetaModel() || !parentMm.getChildMetaModels().isEmpty()) {
			parentMm.setHasChildMetaModel(true);
		}
		return model;
	}

	/**
	 * 根据当前元数据以及其父亲元数据判断当前元数据是否已经存在于列表中
	 * 
	 * @param metadatas
	 *            已存在的元数据列表
	 * @param metadata
	 *            当前元数据
	 * @param parentMd
	 *            当前元数据的父亲元数据
	 * @return
	 */
	private boolean isExist(List metadatas, MMMetadata metadata, MMMetadata parentMd) {
		boolean isContain = false;
		if (parentMd instanceof TemplateMetadata) {
			metadatas = ((TemplateMetadata) parentMd).getChildren();
		}
		label: for (Iterator<MMMetadata> it = metadatas.iterator(); it.hasNext();) {
			MMMetadata md = (MMMetadata) it.next();
			if (!md.getClassifierId().equals(metadata.getClassifierId())) {
				continue;
			}
			if (isEquals(md, metadata)) {
				isContain = true;
				MMMetadata mdPar = md.getParentMetadata();
				MMMetadata metadataPar = parentMd;
				while (mdPar != null) {
					if (isEquals(mdPar, metadataPar)) {
						mdPar = mdPar.getParentMetadata();
						metadataPar = metadataPar.getParentMetadata();
						isContain = true;
						if (mdPar == null) {
							break label;
						}
					} else {
						isContain = false;
						continue label;
					}
				}
			}
		}
		return isContain;
	}

	/**
	 * 根据当前元数据以及其父亲元数据判断当前元数据是否已经存在于列表中
	 * 
	 * @param metadatas
	 *            已存在的元数据列表
	 * @param metadata
	 *            当前元数据
	 * @param parentMd
	 *            当前元数据的父亲元数据
	 * @return
	 */
	private MMMetadata findExist(List metadatas, MMMetadata metadata, MMMetadata parentMd) {
		boolean isContain = false;
		MMMetadata md = null;
		if (parentMd instanceof TemplateMetadata) {
			metadatas = ((TemplateMetadata) parentMd).getChildren();
		}
		label: for (Iterator<MMMetadata> it = metadatas.iterator(); it.hasNext();) {
			md = (MMMetadata) it.next();
			if (!md.getClassifierId().equals(metadata.getClassifierId())) {
				continue;
			}
			if (isEquals(md, metadata)) {
				isContain = true;
				MMMetadata mdPar = md.getParentMetadata();
				MMMetadata metadataPar = parentMd;
				while (mdPar != null) {
					if (isEquals(mdPar, metadataPar)) {
						mdPar = mdPar.getParentMetadata();
						metadataPar = metadataPar.getParentMetadata();
						isContain = true;
						if (mdPar == null) {
							break label;
						}
					} else {
						isContain = false;
						continue label;
					}
				}
			}
		}

		if (isContain) {
			return md;
		}

		return null;
	}

	/**
	 * 比较当前两个元数据的code和classifierId是否相同
	 * 
	 * @param metadata1
	 * @param metadata2
	 * @return
	 */
	private boolean isEquals(MMMetadata metadata1, MMMetadata metadata2) {
		boolean isEqual = false;
		if (metadata1.getCode().equals(metadata2.getCode())
				&& metadata1.getClassifierId().equals(metadata2.getClassifierId())) {
			isEqual = true;
		}
		return isEqual;
	}

	/**
	 * 根据标题的名称获取该标题所在的sheet，返回sheetNode的列表
	 * 
	 * @param document
	 * @param title
	 *            标题的名称
	 * @param level
	 *            标题的级别
	 * @return list 返回该标题所在的sheet，可能是1或多个sheet
	 * 
	 */
	private List<Element> parseTitleColumn(Document document, TemplateTitleJoint titleJoint, int level) {
		// title = "结构路径_结构英文名";
		// level = 2;//第几级标题就表示该标题在sheet上面的第几行
		List<TemplateTitle> titles = titleJoint.getTitles();
		List<Element> worksheetList = new ArrayList<Element>();
		List worksheets = document.selectNodes("/*[name()='Workbook']/*[name()='Worksheet']"
				+ "/*[name()='Table']/*[name()='Row']/*[name()='Cell']" + "/*[contains(name(),'Data')][text()=\'"
				+ titles.get(titles.size() - 1).getName() + "\']" + "/parent::*/parent::*/parent::*/parent::*");

		for (Iterator iter = worksheets.iterator(); iter.hasNext();) {
			Element worksheetNode = (Element) iter.next();
			String columnIndex = getColumnIndex(worksheetNode, titleJoint, level);
			if (columnIndex != null) {
				worksheetList.add(worksheetNode);
			}
		}

		return worksheetList;
	}

	/**
	 * 获得一个标题的结束行，或是标题所在的行数 如果标题是2级以上则获得标题的结束行，如果标题只有1级则获得标题所在的行数
	 * 
	 * @param worksheetNode
	 * @param title
	 * @return
	 */
	private int getRowIndexByTitle(Node worksheetNode, TemplateClsTitle title) {
		List<TemplateTitle> firstCodeTitle = title.getMdCode().getTitleJoints().get(0).getTitles();
		String lastLevelTitle = firstCodeTitle.get(firstCodeTitle.size() - 1).getName();
		Node lastLevelTitleNode = worksheetNode.selectSingleNode("*[name()='Table']/*[name()='Row']/*[name()='Cell']/"
				+ "*[contains(name(),'Data')][text()=\'" + lastLevelTitle + "\']/parent::*/parent::*");
		return getRowNumberByRowNode(lastLevelTitleNode);
	}

	/**
	 * 获得数据的开始行
	 * 
	 * @param worksheetNode
	 * @param title
	 * @return
	 */
	private int getDataStartIndex(Node worksheetNode, TemplateClsTitle title) {
		return getRowIndexByTitle(worksheetNode, title) + 1;
	}

	/**
	 * 根据结束标志获得该worksheet的结束行
	 * 
	 * @param worksheetNode
	 *            worksheet
	 * @param endFlag
	 *            结束标志
	 * @return
	 */
	private int getDataEndIndex(Node worksheetNode, String endFlag) {
		int end = worksheetNode.selectNodes("*[name()='Table']/*[name()='Row']").size();

		Node endRowNode = worksheetNode.selectSingleNode("*[name()='Table']/*[name()='Row']/*[name()='Cell']/"
				+ "*[contains(name(),'Data')][text()=\'" + endFlag + "\']/parent::*/parent::*");

		if (endFlag != null && endFlag.length() > 0 && endRowNode != null) {
			end = getRowNumberByRowNode(endRowNode) - 1;
		}

		for (int i = end; i >= 1; i--) {
			Node rowNode = worksheetNode.selectSingleNode("*[name()='Table']/*[name()='Row'][" + i + "]");
			if (rowNode != null) {
				// 匹配行，如果不是空行则结束
				if (!isBlank(rowNode.getStringValue())) {
					end = i;
					break;
				}
			}
		}
		return end;
	}

	/**
	 * 根据行节点获取该行节点所在的行数
	 * 
	 * @param rowNode
	 * @return
	 */
	private int getRowNumberByRowNode(Node rowNode) {
		String rowNodePath = rowNode.getUniquePath();
		int sLastIndex = rowNodePath.lastIndexOf("[");
		int eLastIndex = rowNodePath.lastIndexOf("]");
		String aRow = rowNodePath.substring(sLastIndex + 1, eLastIndex);
		int rowNum = 1;
		try {
			rowNum = Integer.valueOf(aRow).intValue();
		} catch (NumberFormatException e) {
			// 当sheet页面有且只有一个行的时候，[name()='row']后面不会有数组下标，
			// 自然取不出行数，例如[name()='row'][1]取出行数1
			log.debug("can't find the data row! [" + aRow + "]");
			System.out.println("can't find the data row! [" + aRow + "]");
		}

		return rowNum;
	}

	/**
	 * 判断字符串是否为空，如果是空行返回true
	 * 
	 * @param string
	 * @return
	 */
	private boolean isBlank(String string) {
		Pattern pattern = Pattern.compile("\n[\\s| ]*|\n[\\s| ]*\r");
		Matcher matcher = pattern.matcher(string);

		return matcher.matches();
	}

	/**
	 * Inner class
	 * 
	 * @author Administrator
	 * 
	 */
	private class MetaTemplateDep {
		private MMMetadata depMd;

		List<TemplateDep> depedList;

		List<TemplateComp> depList;

		private Node currentWorksheet;

		private int level;

		private int row;

		public MMMetadata getDepMd() {
			return depMd;
		}

		public void setDepMd(MMMetadata depMd) {
			this.depMd = depMd;
		}

		public List<TemplateDep> getDepedList() {
			return depedList;
		}

		public List<TemplateComp> getDepList() {
			return depList;
		}

		public void setDepList(List<TemplateComp> depList) {
			this.depList = depList;
		}

		public void setDepedList(List<TemplateDep> depedList) {
			this.depedList = depedList;
		}

		public Node getCurrentWorksheet() {
			return currentWorksheet;
		}

		public void setCurrentWorksheet(Node currentWorksheet) {
			this.currentWorksheet = currentWorksheet;
		}

		public int getLevel() {
			return level;
		}

		public void setLevel(int level) {
			this.level = level;
		}

		public int getRow() {
			return row;
		}

		public void setRow(int row) {
			this.row = row;
		}
	}

}
