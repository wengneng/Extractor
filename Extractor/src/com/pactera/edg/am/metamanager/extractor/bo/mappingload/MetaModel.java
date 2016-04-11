package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

import java.io.Serializable;
import java.util.Map;

/**
 * 元模型domain
 * 
 * @author user
 * @version 1.0
 * 
 */
public class MetaModel implements Comparable<MetaModel>, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 982342378231L;

	// 元模型名称
	private String mName;

	// 元模型显示名
	private String displayName;

	// 元模型的父结点
	private MetaModel parentMetaModel;

	// 元模型的子结点
	// private List<MetaModel> childMetaModels;

	// 元模型所属的命名空间
	private String namespace;

	// 元模型包含的属性:key-->元模型的属性名;value-->元模型的属性所在元数据表的字段名
	private Map<String, String> mAttrs;
	
	private Long uuid;

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getMName() {
		return mName;
	}

	public void setMName(String name) {
		mName = name;
	}

	public MetaModel getParentMetaModel() {
		return parentMetaModel;
	}

	public void setParentMetaModel(MetaModel parentMetaModel) {
		this.parentMetaModel = parentMetaModel;
	}

	// public List<MetaModel> getChildMetaModels() {
	// return childMetaModels;
	// }
	//
	// public void setChildMetaModels(List<MetaModel> childMetaModels) {
	// this.childMetaModels = childMetaModels;
	// }

	public Map<String, String> getMAttrs() {
		return mAttrs;
	}

	public void setMAttrs(Map<String, String> attrs) {
		mAttrs = attrs;
	}

	public boolean equals(Object o) {
		if (!(o instanceof MetaModel)) {
			return false;
		}
		MetaModel mm = (MetaModel) o;

		// 命名空间不相同，或者元模型名不相同，都会返回false,否则返回true
		return namespace.equals(mm.getNamespace())
				&& mName.equals(mm.getMName());
	}

	public int compareTo(MetaModel o) {
		int compareInt = namespace.compareTo(o.getNamespace());
		if(compareInt == 0){
			return mName.compareTo(o.getMName());
		}
		return compareInt;
	}

	public Long getUuid() {
		return uuid;
	}

	public void setUuid(Long uuid) {
		this.uuid = uuid;
	}

}
