/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.core.ex.BizException;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.ICommonExtractService;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * @author wangyang
 * 
 */
public class AnnotateMappingServiceImpl extends BaseMappingServiceImpl
		implements IMetadataMappingService {
	private Log log = LogFactory.getLog(getClass());

	private static Map<String, String> ann = new HashMap<String, String>();

	private static String[] notNullAnn = { "functionName", "srcTables",
			"targetTables" };
	static {
		ann.put("functionName", "作业名称");// 必填
		ann.put("srcTables", "源表");// 必填
		ann.put("targetTables", "目标表");// 必填
		ann.put("copyRight", "版权信息");
		ann.put("responsiblePerson", "责任人");
		ann.put("mappingFile", "需求来源");
		ann.put("loadPolicy", "加载策略");
		ann.put("remarks", "备注");
		ann.put("versionNo", "版本号");
		ann.put("modifyHistory", "修改历史");
	}

	private List<MMDDependency> dDependencies = new ArrayList<MMDDependency>();

	private List schemas = new ArrayList();

	private List tables = new ArrayList();

	private List models = new ArrayList();

	private final String AnnotateMmCode = "EtlAnnotate";

	private final String SchemaMmCode = Schema.class.getSimpleName();

	private final String TableMmCode = Table.class.getSimpleName();

	/**
	 * 注释源数据提供接口
	 */
	private ICommonExtractService annotateExtractService;

	public void setAnnotateExtractService(
			ICommonExtractService annotateExtractService) {
		this.annotateExtractService = annotateExtractService;
	}

	public void metadataMapping(AppendMetadata metadata) throws Exception {
		BufferedReader br = null;
		try {
			MMMetadata appendMd = metadata.getMetadata();
			MMMetaModel appendMm = metadata.getMetaModel();
			InputStream is = annotateExtractService.extract();
			br = new BufferedReader(new InputStreamReader(is));

			// 解析注释元数据
			MMMetaModel annModel = getModel(models, AnnotateMmCode, appendMm);
			MMMetadata annMd = parseAnnMd(br, annModel, appendMd);

			// 对源表、目标表进行处理
			String srcTablesStr = annMd.getAttr("srcTables");
			String targetTablesStr = annMd.getAttr("targetTables");
			List<MMMetadata> srcTabMds = getTableList(srcTablesStr, appendMd,
					appendMm);
			List<MMMetadata> targetTabMds = getTableList(targetTablesStr,
					appendMd, appendMm);

			// 建立注释模型元数据与源表、目标表依赖关系
			addDepRela(annMd, null, srcTabMds, "source");
			addDepRela(annMd, null, targetTabMds, "target");
			isUnique();
			metadata.setDDependencies(dDependencies);
			metadata.setChildMetaModels(appendMm.getChildMetaModels());
			metadata.setMetaModel(appendMm);
		} catch (Exception e) {
			throw e;
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}

	private MMMetaModel getModel(List models, String modelCode,
			MMMetaModel parentMm) {
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
		if (parentMm.isHasChildMetaModel()
				|| !parentMm.getChildMetaModels().isEmpty()) {
			parentMm.setHasChildMetaModel(true);
		}
		return model;
	}

	private void addMd2Mm(MMMetaModel model, MMMetadata metadata) {
		if (!isContainMd(model.getMetadatas(), metadata)) {
			model.addMetadata(metadata);
		}
		if (model.isHasMetadata() || !model.getMetadatas().isEmpty()) {
			model.setHasMetadata(true);
		}
	}

	private MMMetadata parseAnnMd(BufferedReader br, MMMetaModel annModel,
			MMMetadata appendMd) throws IOException {
		MMMetadata annMd = new MMMetadata();
		annMd.setClassifierId(annModel.getCode());
		annMd.setParentMetadata(appendMd);
		
		appendMd.addChildMetadata(annMd);
		// 是否注释
		boolean isAnn = false;
		String lineText = br.readLine();
		String lastAnnKey = null;

		// 存储过程内容
		StringBuilder procText = new StringBuilder();
		while (lineText != null) {
			procText.append(lineText).append("\r\n");

			boolean isNewAnn = false;
			if (lineText.startsWith("/*==")) {
				isAnn = true;
				lineText = br.readLine();
				continue;
			}
			if (lineText.endsWith("==*/")) {
				break;// 注释已经读取完毕，不再读取,TODO:是否只能支持一个注释块？
			}
			if (isAnn) {
				for (Iterator it = ann.entrySet().iterator(); it.hasNext();) {
					Entry next = (Entry) it.next();
					String annKey = (String) next.getKey();// 注释项属性
					String annCn = (String) next.getValue();// 注释项中文
					String annCnM = annCn + ":";// 带上冒号的

					// 全角的冒号转成英文形式的
					lineText = lineText.replaceFirst("：", ":");

					if (StringUtils.indexOf(lineText, annCnM) != -1) {
						isNewAnn = true;
						// 取得冒号后的文本作为值，规定一行之内，不会存在两个注释项
						String annValue = lineText.substring(lineText
								.indexOf(annCnM)
								+ annCnM.length(), lineText.length());
						annMd.addAttr(annKey, annValue);
						lastAnnKey = annKey;
						break;
					}
				}
				if (!isNewAnn) {
					String val = annMd.getAttr(lastAnnKey);
					val += lineText;
					annMd.getAttrs().remove(lastAnnKey);
					annMd.addAttr(lastAnnKey, val);
				}
			}
			lineText = br.readLine();
		}
		annMd
				.addAttr(
						"functionDescription",
						procText.length() > AdapterExtractorContext.getInstance().getMaxMetadataAttrSize() ? procText
								.toString().substring(0,
										AdapterExtractorContext.getInstance().getMaxMetadataAttrSize())
								.concat("...")
								: procText.toString());

		validNotNull(annMd);
		annMd.setName(annMd.getAttr("functionName"));
		annMd.setCode(annMd.getAttr("functionName"));
		annMd.getAttrs().remove("functionName");
		addMd2Mm(annModel, annMd);
		return annMd;
	}

	private void validNotNull(MMMetadata md) throws BizException {
		// 检查必填项
		List<String> exList = new ArrayList<String>();
		boolean isThrow = false;
		for (int i = 0; i < notNullAnn.length; i++) {
			String annKey = notNullAnn[i];
			if (!md.getAttrs().containsKey(annKey)) {
				exList.add(ann.get(annKey));
				isThrow = true;
			}
		}
		StringBuffer exText = new StringBuffer();
		for (int i = 0; i < exList.size(); i++) {
			String exCn = exList.get(i);
			if (i == exList.size() - 1) {
				exText.append(exCn);
			} else {
				exText.append(exCn + ",");
			}
		}
		if (isThrow) {
			throw new BizException("注释项：" + exText + " 缺失");
		}
	}

	private List<MMMetadata> getTableList(String tableStr, MMMetadata appendMd,
			MMMetaModel appendMm) {
		// 把schema.table转换成大写的
		tableStr = StringUtils.upperCase(tableStr);
		String[] tabs = StringUtils.splitPreserveAllTokens(tableStr, ",");
		return parseTableMd(tabs, appendMd, appendMm);
	}

	private List<MMMetadata> parseTableMd(String[] tabs, MMMetadata appendMd,
			MMMetaModel appendMm) {
		List<MMMetadata> mds = new ArrayList<MMMetadata>();
		for (int i = 0; i < tabs.length; i++) {
			String st = tabs[i];
			if (st.indexOf(".") == -1 || StringUtils.countMatches(st, ".") > 1) {
				throw new BizException("无法取得表所属的Schema");
			}
			String[] sts = StringUtils.split(st, ".");
			String schemaCode = sts[0].trim();
			String tableCode = sts[1].trim();
			MMMetaModel schemaMm = getModel(models, SchemaMmCode, appendMm);
			MMMetaModel tableMm = getModel(models, TableMmCode, schemaMm);
			MMMetadata schemaMd = getMd(schemas, schemaCode, schemaMm, appendMd);
			MMMetadata tableMd = getMd(tables, tableCode, tableMm, schemaMd);
			// 用于创建依赖关系
			if (!isContainMd(mds, tableMd)) {
				mds.add(tableMd);
			}
		}
		return mds;
	}

	private MMMetadata getMd(List mds, String mdCode, MMMetaModel model,
			MMMetadata appendMd) {
		MMMetadata metadata = new MMMetadata();
		metadata.setCode(mdCode);
		metadata.setName(mdCode);
		metadata.setClassifierId(model.getCode());
		metadata.setParentMetadata(appendMd);
		
		appendMd.addChildMetadata(metadata);
		int indexMd = indexMd(mds, metadata);
		if (-1 == indexMd) {
			// 设置模型的元数据
			mds.add(metadata);
			addMd2Mm(model, metadata);
		} else {
			metadata = (MMMetadata) mds.get(indexMd);
		}
		return metadata;
	}

	private void addDepRela(MMMetadata depMd, String depRole,
			List<MMMetadata> depedMds, String depedRole) {
		for (MMMetadata depedMd : depedMds) {
			MMDDependency mmd = new MMDDependency();
			mmd.setOwnerMetadata(depMd);// 依赖端
			mmd.setOwnerRole(depRole);
			mmd.setValueMetadata(depedMd);// 被依赖端
			mmd.setValueRole(depedRole);
			dDependencies.add(mmd);
		}
	}

	private boolean isContainMd(List metadatas, MMMetadata metadata) {
		boolean isContain = false;
		for (Iterator it = metadatas.iterator(); it.hasNext();) {
			MMMetadata md = (MMMetadata) it.next();
			if (md.compareTo(metadata) == 0) {
				isContain = true;
			}
		}
		return isContain;
	}

	private int indexMd(List metadatas, MMMetadata metadata) {
		int index = -1;
		for (int i = 0; i < metadatas.size(); i++) {
			MMMetadata md = (MMMetadata) metadatas.get(i);
			if (md.compareTo(metadata) == 0) {
				index = i;
			}
		}
		return index;
	}

	private void isUnique() {
		for (int i = 0; i < dDependencies.size(); i++) {
			MMDDependency mmd = dDependencies.get(i);
			dpEqual(mmd, i);
		}
	}

	private void dpEqual(MMDDependency mmd, int q) {
		for (int i = 0; i < dDependencies.size(); i++) {
			MMDDependency mmdTemp = dDependencies.get(i);
			if (mmdTemp.getOwnerMetadata() == mmd.getOwnerMetadata()
					&& mmdTemp.getValueMetadata() == mmd.getValueMetadata()
					&& StringUtils.equals(mmdTemp.getOwnerRole(), mmd
							.getOwnerRole())
					&& StringUtils.equals(mmdTemp.getValueRole(), mmd
							.getValueRole()) && i != q) {
				log.info("转换中出现重复的依赖关系：ownerName:"
						+ mmd.getOwnerMetadata().getCode() + "valueName:"
						+ mmd.getValueMetadata().getCode() + "去除重复关系.");
				dDependencies.remove(i);
			}
		}

	}
}
