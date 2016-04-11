package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Element;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control.PcUtils;

public class PcBO implements Cloneable {
	private Log log = LogFactory.getLog(getClass());
	private String id;
	private String code;
	private String name;
	private String type;
	private Map<String, String> attributes = new HashMap<String, String>();
	private List<PcBO> children = new ArrayList<PcBO>();
	private PcBO parent;
	// 该BO与Element的依赖关系
	private Element depElement = null;
	// 依赖关系
	private List<PcDependency> dependencys = new ArrayList<PcDependency>();
	// 保存所有的BO
	private Map<String, PcBO> allPcBOs = new HashMap<String, PcBO>();
	// 保存所有的依赖关系
	private Map<String, PcDependency> allDependencys = new HashMap<String, PcDependency>();

	/**
	 * 新建一个PC实例，并且会将该实例加入父节点的子节点列表中
	 * 
	 * @param parent
	 */
	public PcBO(PcBO parent, Element element, String addString) {
		this.parent = parent;
		if (element != null) {
			id = PcUtils.genXPath(element);
			if (addString != null) {
				id = id + addString;
			}
		} else {
			id = UUID.randomUUID() + "";
		}

		if (parent != null) {
			parent.getChildren().add(this);
			allPcBOs = parent.getAllPcBOs();
			allDependencys = parent.getAllDependencys();
		} else {
			allPcBOs = new HashMap<String, PcBO>();
		}

		// 将自身列表赋值
		allPcBOs.put(id, this);
	}

	/**
	 * 添加Element的依赖关系
	 * 
	 * @param depedId
	 */
	public void setElementDependency(Element depElement) {
		setDepElement(depElement);
		PcDependency pcDependency = new PcDependency(this.getId(), null,
				PcUtils.genXPath(depElement), null);
		String dependencyKey = PcUtils.genDependencyKey(pcDependency);
		if (!allDependencys.containsKey(dependencyKey)) {
			allDependencys.put(dependencyKey, pcDependency);
		}
	}

	/**
	 * 添加依赖关系
	 * 
	 * @param pcDependency
	 */
	public void addDependency(String toId, String toRole) {
		PcDependency pcDependency = new PcDependency(id, null, toId, toRole);
		String dependencyKey = PcUtils.genDependencyKey(pcDependency);
		if (!allDependencys.containsKey(dependencyKey)) {
			dependencys.add(pcDependency);
			allDependencys.put(PcUtils.genDependencyKey(pcDependency),
					pcDependency);
		}

	}

	/**
	 * 根据code和type查询子节点
	 * 
	 * @param code
	 * @param type
	 */
	public PcBO searchChildByCodeAndType(String code, String type) {
		for (int i = 0; i < getChildren().size(); i++) {
			if (getChildren().get(i).getType().equals(type)
					&& getChildren().get(i).getCode().equals(code)) {
				return getChildren().get(i);
			}
		}
		return null;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
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

	public List<PcBO> getChildren() {
		return children;
	}

	public void setChildren(List<PcBO> children) {
		this.children = children;
	}

	public PcBO getParent() {
		return parent;
	}

	public void setParent(PcBO parent) {
		this.parent = parent;
	}

	public Map<String, PcBO> getAllPcBOs() {
		return allPcBOs;
	}

	public void setAllPcBOs(Map<String, PcBO> allPcBOs) {
		this.allPcBOs = allPcBOs;
	}

	public Element getDepElement() {
		return depElement;
	}

	public void setDepElement(Element depElement) {
		this.depElement = depElement;
	}

	public List<PcDependency> getDependencys() {
		return dependencys;
	}

	public void setDependencys(List<PcDependency> dependencys) {
		this.dependencys = dependencys;
	}

	public Map<String, PcDependency> getAllDependencys() {
		return allDependencys;
	}

	public void setAllDependencys(Map<String, PcDependency> allDependencys) {
		this.allDependencys = allDependencys;
	}

	public Object clone() {
		try {
			return super.clone();

		} catch (CloneNotSupportedException e) {
			return null;
		}
	}
}
