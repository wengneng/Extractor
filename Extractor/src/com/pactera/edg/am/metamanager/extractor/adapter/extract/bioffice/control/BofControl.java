package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.control;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bof.catalogtree.ICatalogElement;
import bof.sdk.ClientConnector;
import bof.sdk.service.catalog.CatalogService;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofDependency;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.parse.AbstractParse;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.webService.BofConnector;

public class BofControl {
	private Log log = LogFactory.getLog(getClass());
	private Map<String, AbstractParse> parsers = new HashMap<String, AbstractParse>();
	private BofConnector bofConnector;
	private ClientConnector conn;
	private CatalogService catalogService;

	public BofControl(BofConnector bofConnector) {
		this.bofConnector = bofConnector;
		//打开连接
		this.conn=bofConnector.getConn();
		//获取服务
		this.catalogService=new CatalogService(conn);
	}

	public BofBO actionParse() {	
		
		// 悬挂点
		BofBO bofRespostory = new BofBO(null, "0");
		bofRespostory.setCode("bofRespostory");
		bofRespostory.setName("bofRespostory");
		bofRespostory.setType("RESPOSITORY");

		// 获取所有根目录
		List<ICatalogElement> roots = catalogService.getRootElements();

		// 解析根目录
		for (int i = 0; i < roots.size(); i++) {
			ICatalogElement root = roots.get(i);
			if (ParseTypeHelper.isRoot(root.getType())) {
				log.info("开始解析目录：" + root.getName());
				// 开始递归解析
				parseBof(root, bofRespostory);
			}
		}

		// 汇总表级依赖关系
		sumTableLevelDependency(bofRespostory.getAllDependencys(),
				bofRespostory.getBofBOs());
		
		//关闭连接
		this.conn.close();
		
		bofConnector.closeConn();
		
		return bofRespostory;
	}

	/**
	 * 递归解析
	 * 
	 * @param element
	 * @param parentBO
	 * @return
	 */
	private void parseBof(ICatalogElement element, BofBO parent) {
		// 判断该类型是否在解析范围内
		if (ParseTypeHelper.existInstanceType(element.getType())) {
			AbstractParse abstractParse;
			// 获取该类型的解析器
			abstractParse = getParser(element.getType());

			// 解析
			BofBO bofBO = abstractParse.parse(parent, element);

			// 解析子节点
			List<? extends ICatalogElement> children = catalogService
					.getChildElements(element.getId());
			for (int i = 0; i < children.size(); i++) {
				parseBof(children.get(i), bofBO);
			}

		}
	}

	/**
	 * 汇总表级依赖关系
	 * 
	 * @param allDependencys
	 * @param allBofBOs
	 */
	private void sumTableLevelDependency(
			Map<String, BofDependency> allDependencys,
			Map<String, BofBO> allBofBOs) {
		Map<String, BofDependency> talbeDependencys = new HashMap<String, BofDependency>();
		for (Iterator iterator = allDependencys.keySet().iterator(); iterator
				.hasNext();) {
			BofDependency bofDependency = allDependencys.get((String) iterator
					.next());
			// 如果是字段级依赖关系，生成表级依赖关系
			BofBO depedBO = allBofBOs.get(bofDependency.getToId());
			BofBO depBO = allBofBOs.get(bofDependency.getFromId());
			if (depedBO != null) {
				if (ParseTypeHelper.getBofType(depBO.getType()).isFieldLevel()) {
					talbeDependencys.put("[" + depBO.getParent().getId() + "]"
							+ "[" + depedBO.getParent().getId() + "]",
							new BofDependency(depBO.getParent().getId(),
									depedBO.getParent().getId()));
				}
			} else {
				log.debug("没有取到ID为" + bofDependency.getToId() + "的BO");
			}
		}

		allDependencys.putAll(talbeDependencys);
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
				abstractParse = (AbstractParse) ParseTypeHelper
						.getBofType(type).getParseClass().newInstance();
				abstractParse.init(bofConnector);
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

	public CatalogService getCatalogService() {
		return catalogService;
	}

	public void setCatalogService(CatalogService catalogService) {
		this.catalogService = catalogService;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}

	public BofConnector getBofConnector() {
		return bofConnector;
	}

	public void setBofConnector(BofConnector bofConnector) {
		this.bofConnector = bofConnector;
	}
}
