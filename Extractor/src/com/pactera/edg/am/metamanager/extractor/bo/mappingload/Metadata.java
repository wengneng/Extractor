package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * 元数据对象，可实现元数据比较及排序策略
 *
 * @author user
 * @version 1.0  Date: Jul 7, 2009
 *
 */
public class Metadata implements Comparable<Metadata>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 39870937893980L;

	// 元数据名称
	private String dName;

	// 元数据显示名
	private String displayName;

	// 元数据全路径的hashCode
	private int uuPathCode;

	// 元数据的父结点
	private Metadata parentMetadata;

	// 父结点是否存在于存储库中
	private boolean existInDB;

	// 元数据所属的元模型
	private MetaModel ownerMetaModel;

	// 元数据包含的属性
	private Map<String, String> dAttrs;

	// 元数据的唯一ID
	private long uuid;

	public boolean isExistInDB() {
		return existInDB;
	}

	public void setExistInDB(boolean existInDB) {
		this.existInDB = existInDB;
	}

	public int getUuPathCode() {
		return uuPathCode;
	}

	public void setUuPathCode(int uuPathCode) {
		this.uuPathCode = uuPathCode;
	}

	public long getUuid() {
		return uuid;
	}

	public void setUuid(long uuid) {
		this.uuid = uuid;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public Metadata getParentMetadata() {
		return parentMetadata;
	}

	public void setParentMetadata(Metadata parentMetadata) {
		this.parentMetadata = parentMetadata;
	}

	public MetaModel getOwnerMetaModel() {
		return ownerMetaModel;
	}

	public void setOwnerMetaModel(MetaModel ownerMetaModel) {
		this.ownerMetaModel = ownerMetaModel;
	}

	public String getDName() {
		return dName;
	}

	public void setDName(String name) {
		dName = name;
	}

	public Map<String, String> getDAttrs() {
		return dAttrs;
	}

	public void setDAttrs(Map<String, String> attrs) {
		dAttrs = attrs;
	}

	// 获取元数据的一个属性，如不存在该属性则返回null
	public String getDAttr(String key) {
		return getDAttrs().get(key);
	}

	// 添加元数据的一个属性值
	public boolean addDAttr(String key, String value) {
		if (!getDAttrs().containsKey(key)) {
			getDAttrs().put(key, value);
			return true;
		}
		return false;
	}

	// 删除元数据的一个属性值
	public void delDAttr(String key) {
		if (getDAttrs().containsKey(key)) {
			getDAttrs().remove(key);
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("元数据全路径[").append(uuPathCode).append("], 元数据属性\n[");

		Iterator<String> keys = dAttrs.keySet().iterator();
		String key = null;
		while (keys.hasNext()) {
			key = keys.next();
			sb.append(key).append("=").append(dAttrs.get(key)).append("\n");
		}
		sb.append("]");
		return sb.toString();
	}

	/**
	 * 实现元数据比较策略
	 */
	public int compareTo(Metadata o) {
		// 元数据全路径作比较
		int compareInt = uuPathCode - o.getUuPathCode();
		if (compareInt == 0) {
			// 路径相同，再判断其所属元模型
			return ownerMetaModel.compareTo(o.getOwnerMetaModel());
		}
		return compareInt;
	}
	
	/**
	 * 对相同元数据的属性作比较
	 * 产生一个需要修改的元数据，它包含：元数据ID;该元数据所有添加，修改的属性放入属性增加列表中;删除的属性放入删除列表中
	 * @param o
	 * @return
	 */
	public ModifyMetadata compareAttrTo2(Metadata o){
		Iterator<String> keys = dAttrs.keySet().iterator();
		String key = null;
		String value = null;
		/**
		 * 增加的属性，同时将修改的属性也放入此中
		 */
		Map<String, String> addAttrs = new HashMap<String, String>();
		/**
		 * 删除的属性
		 */
		Map<String, String> delAttrs = new HashMap<String, String>();
		
		while (keys.hasNext()) {
			key = keys.next();
			if (!o.getDAttrs().containsKey(key)) {
				// 新数据中不存在此key,则认为其需要删除s
				delAttrs.put(key, dAttrs.get(key));
				continue;
			}
			
			value = o.getDAttr(key);

			if (!value.equals(dAttrs.get(key))) {
				// 修改的属性
				addAttrs.put(key, value);
			}
		}
		
		// 获取增加的属性
		addAttrs.putAll(getCreateAttrs(o));
		
		Map<Operation, Map<String, String>> modifyAttrs = new HashMap();
		if(addAttrs.size() > 0){
			modifyAttrs.put(Operation.CREATE, addAttrs);
			
		}
		if(delAttrs.size() > 0){
			modifyAttrs.put(Operation.DELETE, delAttrs);
		}

		if(modifyAttrs.size() > 0){
			ModifyMetadata modifyMetadata = new ModifyMetadata();
			modifyMetadata.setUuid(uuid);
			modifyMetadata.setModifyAttrs(modifyAttrs);
			
			return modifyMetadata;
		}
		return null;
	}

	/**
	 * 对相同元数据的属性作比较，属性值不相同则认为需要更新属性
	 * 分比较元数据与被比较元数据，比较元数据中存在的key,在被比较元数据中不存在的属性，将其放入删除列表中；
	 * 比较元数据中存在的key，同时在被比较元数据中也存在，但value不等的，将其放入修改列表中；
	 * 比较元数据中不存在，但在被比较元数据中存在的key,将其放入添加列表中。
	 * 
	 * @param o
	 *            Metadata 被比较元数据
	 * @return Map<Operation, Map<String, String>>
	 *         Operation可为添加，修改，删除，用于存放三种操作的元数据属性
	 */
	public Map<Operation, Map<String, String>> compareAttrTo(Metadata o) {
		Iterator<String> keys = dAttrs.keySet().iterator();
		String key = null;
		String value = null;
		// 删除的元数据属性
		Map<String, String> delAttrs = new HashMap<String, String>();
		// 修改的元数据属性
		Map<String, String> modifyAttrs = new HashMap<String, String>();

		while (keys.hasNext()) {
			key = keys.next();
			if (!o.getDAttrs().containsKey(key)) {
				// 1.比较元数据中存在的key,在被比较元数据中不存在的属性，将其放入删除列表中；
				delAttrs.put(key, dAttrs.get(key));
				continue;
			}

			value = o.getDAttr(key);

			if (!value.equals(dAttrs.get(key))) {
				// 2.比较元数据中存在的key，同时在被比较元数据中也存在，但value不等的，将其放入修改列表中；
				modifyAttrs.put(key, value);
			}
		}

		// 3.比较元数据中不存在，但在被比较元数据中存在的key,将其放入添加列表中。
		Map<String, String> addAttrs = getCreateAttrs(o);

		// 用于存放添加，修改，删除的元数据属性
		Map<Operation, Map<String, String>> updateDAttrs = new HashMap();
		if (addAttrs.size() > 0) {
			updateDAttrs.put(Operation.CREATE, addAttrs);
		}
		if (modifyAttrs.size() > 0) {
			updateDAttrs.put(Operation.MODIFY, modifyAttrs);
		}
		if (delAttrs.size() > 0) {
			updateDAttrs.put(Operation.DELETE, delAttrs);
		}

		return updateDAttrs;
	}

	/**
	 * 比较新旧数据，产生新增的元数据属性
	 * @param o Metadata
	 * @return
	 */
	private Map<String, String> getCreateAttrs(Metadata o) {
		Iterator<String> ownerKeys = o.getDAttrs().keySet().iterator();
		// 添加的元数据属性
		Map<String, String> addAttrs = new HashMap<String, String>();
		String ownerKey = null;
		while (ownerKeys.hasNext()) {
			ownerKey = ownerKeys.next();
			if (!dAttrs.containsKey(ownerKey)) {
				// 3.比较元数据中不存在，但在被比较元数据中存在的key,将其放入添加列表中。
				addAttrs.put(ownerKey, o.getDAttr(ownerKey));
			}
		}
		return addAttrs;
	}

	public int hashCode() {
		int result = 17;
		result = 37 * result + dName.hashCode();
		result = 37 * result + displayName.hashCode();
		result = 37 * result + uuPathCode;
		result = 37 * result + ownerMetaModel.hashCode();
		return result;
	}

	/**
	 * 比较两个元数据是否为同一元数据
	 * 须满足：元数据全路径相同；所属元模型相同；属性及属性值相同
	 */
	public boolean equals(Object o) {
		if (!(o instanceof Metadata)) {
			// 非Metadata，不相等，返回false
			return false;
		}
		Metadata metadata = (Metadata) o;
		if (uuPathCode != metadata.getUuPathCode()) {
			// 全路径不相同，返回false
			return false;
		} else if (!ownerMetaModel.equals(metadata.getOwnerMetaModel())) {
			// 所属元模型不相同，返回false
			return false;
		} else {
			// 比较属性是否全等
			return attrsEquals(metadata);

		}
	}

	/**
	 * 判断两元数据属性是否一致
	 * 1.遍历元数据A的属性，如有不存在于元数据B中的属性，则不一致返回FALSE；
	 * 2.如属性A.a存在于元数据B中，但属性值不一致，则返回FALSE;
	 * 3.遍历元数据B的属性，如有不存在于元数据A中的属性，则不一致返回FALSE
	 * (走到这一步，对属性相同的情况已无需再判断，因第2步已判断完毕).
	 * @param metadata
	 * @return
	 */
	private boolean attrsEquals(Metadata metadata) {
		Iterator<String> keys = dAttrs.keySet().iterator();
		String key = null;
		String value = null;

		while (keys.hasNext()) {
			key = keys.next();
			if (!metadata.getDAttrs().containsKey(key)) {
				// 1.比较元数据中存在的属性，在被比较元数据中不存在该属性
				return false;
			}
			value = metadata.getDAttr(key);

			if (!value.equals(dAttrs.get(key))){
				// 2.比较元数据与被比较元数据存在相同的属性，但属性值不相同，返回false
				return false;
			}
		}

		Iterator<String> ownerKeys = metadata.getDAttrs().keySet().iterator();
		String ownerKey = null;
		while (ownerKeys.hasNext()) {
			ownerKey = ownerKeys.next();
			if (!dAttrs.containsKey(ownerKey)) {
				// 3.被比较元数据中存在的属性，在比较元数据中不存在该属性
				return false;
			}
		}
		// 全路径，所属元模型，属性都相同，认为两个元数据相同，返回true
		return true;
	}

}
