package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * bi.office bo
 * 
 * @author 游林杰
 * 
 */
public class BofBO {
	private Log log = LogFactory.getLog(getClass());
	private String id;
	private String code;
	private String name;
	private String type;
	private Map<String, String> attributes = new HashMap<String, String>();
	private List<BofBO> children = new ArrayList<BofBO>();
	private BofBO parent;
	// 该BO的依赖关系
	private List<String> dependencys = new ArrayList<String>();
	// 保存所有的BO
	private Map<String, BofBO> allBofBOs = new HashMap<String, BofBO>();
	// 保存所有的依赖关系
	private Map<String, BofDependency> allDependencys = new HashMap<String, BofDependency>();

	/**
	 * 新建一个bi.office实例，并且会将该实例加入父节点的子节点列表中
	 * 
	 * @param parent
	 */
	public BofBO(BofBO parent, String id) {
		this.id = id;
		this.parent = parent;
		// 如果不是悬挂点，父节点中加入子节点
		if (!"0".equals(id)) {
			parent.getChildren().add(this);
		}

		// 将自身加入bo列表中
		Map<String, BofBO> bofBOs;
		if ("0".equals(id)) {
			bofBOs = new HashMap<String, BofBO>();
		} else {
			bofBOs = parent.getBofBOs();
		}
		bofBOs.put(id, this);

		// 对自身列表赋值
		this.allBofBOs = bofBOs;

		// 对所有依赖关系赋值
		if (!"0".equals(id)) {
			this.allDependencys = parent.getAllDependencys();
		}
	}

	/**
	 * 根据ID得到BOfBO
	 * 
	 * @param id
	 * @return
	 */
	public BofBO getBofBOById(String id) {
		return allBofBOs.get(id);
	}

	/**
	 * 添加依赖关系
	 * 
	 * @param depedId
	 *            被依赖端id
	 */
	public void addDependency(String depedId) {
		// 添加依赖关系
		this.getDependencys().add(depedId);
		this.getAllDependencys().put("[" + id + "]" + "[" + depedId + "]",
				new BofDependency(this.id, depedId));

	}
	
	/**
	 * 增加一系列的依赖关系
	 * @param depedIds
	 */
	public void addDependencys(List<String> depedIds){
		for (int i = 0; i < depedIds.size(); i++) {
			this.addDependency(depedIds.get(i));
		}
	}

	/**
	 * 根据CODE得到子对象
	 * 
	 * @param code
	 * @return
	 */
	public BofBO getChildByCode(String code) {
		for (int i = 0; i < children.size(); i++) {
			BofBO bo = children.get(i);
			if (bo.getCode().equals(code)) {
				return bo;
			}
		}
		return null;
	}

	/**
	 * 根据CODE得到子对象,不区分大小写
	 * 
	 * @param code
	 * @return
	 */
	public BofBO getChildByCodeNotCase(String code) {
		for (int i = 0; i < children.size(); i++) {
			BofBO bo = children.get(i);
			if (bo.getCode().toUpperCase().equals(code.toUpperCase())) {
				return bo;
			}
		}
		return null;
	}

	/**
	 * 根据CODE得到子对象,不区分大小写
	 * 
	 * @param code
	 * @return
	 */
	public BofBO getChildByCodeNotCase(String code, String str) {
		for (int i = 0; i < children.size(); i++) {
			BofBO bo = children.get(i);
			String codeStr = StringUtils.remove(bo.getCode(), "\"");
			if (codeStr.toUpperCase().equals(code.toUpperCase())) {
				return bo;
			}
		}
		return null;
	}

	/**
	 * 获取该元数据的上下文路径
	 * 
	 * @return
	 */
	public String getContextPath() {
		String path = "/" + this.getCode();
		BofBO bo = this;

		while (!bo.getParent().getId().equals("0")) {
			path = "/" + bo.getParent().getCode() + path;
			bo = bo.getParent();
		}

		return path;
	}

	/**
	 * 建立间接的依赖关系，如报表与数据源中的表、视图、存储过程、字段
	 * 
	 * @param types
	 *            查询的类型
	 * @param level
	 *            递归层次
	 */
	public void addIndirectlyDep(HashSet<String> types, int maxLevel) {
		int level = 0;
		List<String> depList=new ArrayList<String>();
		this.addIndirectlyDepRecursion(types, maxLevel, level,depList);
		this.addDependencys(depList);
	}

	/**
	 * 递归查询需要建立的关系的bo
	 * 
	 * @param types
	 * @param maxLevel
	 * @param level
	 */
	private void addIndirectlyDepRecursion(HashSet<String> types, int maxLevel,
			int level,List<String> depList) {
		level++;
		List<String> bofBOIds = this.getDependencys();
		for (int i = 0; i < bofBOIds.size(); i++) {
			BofBO depbofBO = this.getBofBOById(bofBOIds.get(i));
			if (level < maxLevel && depbofBO != null) {
				if (types.contains(depbofBO.getType())) {
					depList.add(depbofBO.getId());
				} else {
					depbofBO.addIndirectlyDepRecursion(types, maxLevel,
									level,depList);
				}
			}
		}
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes;
	}

	public List<BofBO> getChildren() {
		return children;
	}

	public BofBO getParent() {
		return parent;
	}

	public Map<String, BofBO> getBofBOs() {
		return allBofBOs;
	}

	/**
	 * 判断依赖关系是否存在
	 * 
	 * @param formId
	 * @param toId
	 * @return
	 */
	public boolean existDependency(String formId, String toId) {
		return getAllDependencys().get("[" + formId + "]" + "[" + toId + "]") != null ? true
				: false;
	}

	public Map<String, BofDependency> getAllDependencys() {
		return allDependencys;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}

	public List<String> getDependencys() {
		return dependencys;
	}

	public void setDependencys(List<String> dependencys) {
		this.dependencys = dependencys;
	}

}
