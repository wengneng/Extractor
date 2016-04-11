package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo.BofBO;

public class CognosBO {
	String id;
	String searchPath;
	String code;
	String name;
	String type;
	CognosBO parent = null;
	Map<String, String> attributes = new HashMap<String, String>();
	List<CognosBO> children = new ArrayList<CognosBO>();
	Map<String, List<String>> depExpressions = new HashMap<String, List<String>>();
	List<CognosBO> dependency = new ArrayList<CognosBO>();

	public CognosBO(CognosBO parent) {
		this.parent = parent;
		// UUID randomUUID = UUID.randomUUID();
		// System.out.println("UUID object:"+randomUUID);
		// id=randomUUID.toString();
		id = Uuid.getUUID();
		// System.out.println("id:" + id);

	}

	public String getSearchPath() {
		return searchPath;
	}

	public void setSearchPath(String searchPath) {
		this.searchPath = searchPath;
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

	public List<CognosBO> getChildren() {
		return children;
	}

	public void setChildren(List<CognosBO> children) {
		this.children = children;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, List<String>> getDepExpressions() {
		return depExpressions;
	}

	public void setDepExpressions(Map<String, List<String>> depExpressions) {
		this.depExpressions = depExpressions;
	}

	public CognosBO getParent() {
		return parent;
	}

	public void setParent(CognosBO parent) {
		this.parent = parent;
	}

	public List<CognosBO> getDependency() {
		return dependency;
	}

	public void setDependency(List<CognosBO> dependency) {
		this.dependency = dependency;
	}

	// 得到根节点
	public CognosBO getRoot() {
		CognosBO parent = this.parent;
		while (parent.getParent() != null) {
			parent = parent.getParent();
		}
		return parent;
	}

	// 根据code和type查询儿子
	public CognosBO queryChildrenByCodeAndType(String code, String type) {
		for (int i = 0; i < this.getChildren().size(); i++) {
			if (getChildren().get(i).getCode().equals(code)
					&& getChildren().get(i).getType().equals(type)) {
				return getChildren().get(i);
			}
		}
		return null;
	}

	/**
	 * 建立间接的依赖关系，如报表与数据源中的表、视图、存储过程、字段
	 * 
	 * @param types
	 *            查询的类型
	 * @param level
	 *            递归层次
	 */
	public void addIndirectlyDep(HashSet<String> types, int maxLevel,List<CognosBO> depList) {
		int level = 0;
		this.addIndirectlyDepRecursion(types, maxLevel, level, depList);
	}

	/**
	 * 递归查询需要建立的关系的bo
	 * 
	 * @param types
	 * @param maxLevel
	 * @param level
	 */
	private void addIndirectlyDepRecursion(HashSet<String> types, int maxLevel,
			int level, List<CognosBO> depList) {
		level++;
		List<CognosBO> cognosBOs = this.dependency;
		for (int i = 0; i < cognosBOs.size(); i++) {
			if (level < maxLevel) {
				if (types.contains(cognosBOs.get(i).getType())) {
					depList.add(cognosBOs.get(i));
				} else {
					cognosBOs.get(i).addIndirectlyDepRecursion(types, maxLevel,
							level, depList);
				}
			}
		}
	}
}
