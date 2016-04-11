package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.catalogtree.ICatalogElement;
import bof.sdk.ClientConnector;
import bof.sdk.service.metadata.DocumentTreeNode;
import bof.sdk.service.metadata.MetadataService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.control.ParseTypeHelper;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.webService.BofConnector;
import com.pactera.edg.am.metamanager.extractor.bo.ExtractorLogLevel;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

public class AbstractParse {
	private BofConnector bofConnector;
	private static Log log = LogFactory.getLog(AbstractParse.class);

	public void init(BofConnector bofConnector) {
		this.bofConnector=bofConnector;
	}

	/**
	 * 解析流程控制
	 * 
	 * @param parent
	 *            父节点
	 * @param element
	 *            接口提供的信息类
	 * @return
	 */
	public BofBO parse(BofBO parent, ICatalogElement element) {

		BofBO bofBO = build(parent, element);

		return bofBO;
	}

	/**
	 * 组装BO
	 * 
	 * @param parent
	 * @param element
	 * @return
	 */
	public BofBO build(BofBO parent, ICatalogElement element) {
		BofBO bofBO = new BofBO(parent, element.getId());

		// 组装基本信息
		bulidBaseInfoFromElement(bofBO, element.getName(), element.getAlias(), element.getType());

		// 解析属性
		buildAttribute(bofBO);

		// 添加依赖关系
		buildDependency(bofBO);

		return bofBO;
	}

	/**
	 * 组装基本信息
	 * 
	 * @param bofBO
	 * @param element
	 * @return
	 */
	protected BofBO bulidBaseInfoFromElement(BofBO bofBO, String name, String alias, String type) {
		bofBO.setCode(name);
		if (alias == null || "".equals(alias)) {
			bofBO.setName(name);
		} else {
			bofBO.setName(alias);
		}
		bofBO.setType(type);
		return bofBO;
	}

	/**
	 * 调用血缘分析接口，添加依赖关系
	 * 
	 * @param bofBO
	 * @param documentTreeNode
	 */
	protected void buildDependency(BofBO bofBO) {
		if (ParseTypeHelper.hasDependency(bofBO.getType())) {
			ClientConnector conn=getBofConnector().getConnByLimitMethod();
			MetadataService metadataService = new MetadataService(conn);
			// 依赖关系分析			
			List<DocumentTreeNode> documentTreeNodes = metadataService.searchReferringTo(bofBO.getId(), false);
			for (int i = 0; i < documentTreeNodes.size(); i++) {
				// 存在依赖关系并且是字段级的
				if (ParseTypeHelper.existDeped(bofBO.getType(), documentTreeNodes.get(i).getType())) {
					// 字段级依赖关系
					bofBO.addDependency(documentTreeNodes.get(i).getId());
					// 表级依赖关系
					// buildTableLevelDependency(bofBO, documentTreeNodes.get(i)
					// .getId());
				}
			}
		}
	}
	
	/**
	 * 从父节点的血缘关系中得到依赖关系
	 * @param bofBO
	 */
	protected void buildDependencyByParent(BofBO bofBO) {
		ClientConnector conn = null;
		try {
			conn = getBofConnector().getConnByLimitMethod();
			if (ParseTypeHelper.hasDependency(bofBO.getType())) {
				// 依赖关系分析,因为字段级的依赖关系在表级中才能查询到，所以查询表级血统关系	
				//metadata服务
				MetadataService metadataService=new MetadataService(conn);
				
				List<DocumentTreeNode> documentTreeNodes = 
						metadataService.searchReferringTo(
								bofBO.getParent().getId(), false);
				
				for (int i = 0; i < documentTreeNodes.size(); i++) {
					// 存在依赖关系并且是字段级的
					if (ParseTypeHelper.existDeped(bofBO.getType(),
							documentTreeNodes.get(i).getType())
							&& bofBO.getCode().equals(
									documentTreeNodes.get(i).getName())) {
						// 字段级依赖关系
						bofBO.addDependency(documentTreeNodes.get(i).getId());
					}
				}
			}
		} catch(Exception e) {
			log.debug("解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
			AdapterExtractorContext.addSQLParserLog(ExtractorLogLevel.WARN,
					"解析Bioffice发生异常,元数据路径：" +bofBO.getContextPath()+";错误信息："+ e.getMessage());
		} 
		
	}

	/**
	 * 添加属性
	 * 
	 * @param bofBO
	 */
	protected void buildAttribute(BofBO bofBO) {
		// bofBO.getAttributes().put("key", "vaule");
	}

	public BofConnector getBofConnector() {
		return bofConnector;
	}

	public void setBofConnector(BofConnector bofConnector) {
		this.bofConnector = bofConnector;
	}
	

}
