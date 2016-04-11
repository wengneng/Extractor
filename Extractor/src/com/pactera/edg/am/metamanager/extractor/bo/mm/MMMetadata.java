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

package com.pactera.edg.am.metamanager.extractor.bo.mm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleCover;
import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 元数据对象
 * 
 * @author user
 * @version 1.0 Date: Jul 27, 2009
 * 
 */
public class MMMetadata extends AbstractMetadata implements Comparable<MMMetadata>, Serializable {
	private Log log = LogFactory.getLog(MMMetadata.class);

	private static final long serialVersionUID = 343446547261578L;

	private String id;

	// 是否已经存在于元数据库表中
	private boolean hasExist;

	// 元数据名
	private String name;

	// 元数据code
	private String code;

	private String description;

	private List<AbstractMetadata> children = new ArrayList<AbstractMetadata>();

	// 元数据属性key,value
	private Map<String, String> attrs = new HashMap<String, String>(0);

	// 元数据父结点
	private MMMetadata parentMetadata;

	// 所属模型的ID
	private String classifierId;

	private int hashCode;

	// namespace
	private String namespace;

	// start-time
	private Long startTime;

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public MMMetadata()
	{
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Map<String, String> getAttrs() {
		return attrs;
	}

	public void setAttrs(Map<String, String> attrs) {
		this.attrs = attrs;
	}

	public void addAttr(String key, String value) {
		attrs.put(key, value);
	}
	
	public void delAttr(String key)
	{
		attrs.remove(key);
	}

	// 获取元数据的一个属性，如不存在该属性则返回null
	public String getAttr(String key) {
		return attrs.get(key);
	}

	public MMMetadata getParentMetadata() {
		return parentMetadata;
	}

	public void setParentMetadata(MMMetadata parentMetadata) {
		this.parentMetadata = parentMetadata;
	}

	/**
	 * 比较两相同元数据的所有属性是否相同,比较规则:判断新元数据的每一个属性,是否在旧元数据中存在,如不存在,则认为属性发生变化;如没变化,则再判断旧数据属性,
	 * 是否有不在新数据中的属性,如有则认为属性发生变化.
	 * 
	 * @param o
	 *            旧数据
	 * @return 如完全相同则返回false,否则返回true
	 */
	public boolean attrsIsDifferent(MMMetadata o) {
		Iterator<String> attrKeys = attrs.keySet().iterator();
		boolean isDifferent = false;
		while (attrKeys.hasNext()) {
			String attrKey = attrKeys.next();
			if (!o.getAttrs().containsKey(attrKey)
					|| (o.getAttrs().containsKey(attrKey) && !attrs.get(attrKey).equals(o.getAttrs().get(attrKey)))) {
				// 1.key存在于新元数据中,但不存在于旧元数据中,需要设置该属性(包括key,value);2.key相同,但value不相同,此时同时需要设置该属性
				isDifferent = true;
				break;
			}
		}

		if (!isDifferent) {
			// 仍然相同,则遍历旧数据的属性,判断是否有不存在于新数据中的属性,如有,则认为两元数据属性不一致
			Iterator<String> oldAttrKeys = o.getAttrs().keySet().iterator();
			while (oldAttrKeys.hasNext()) {
				if (!attrs.containsKey(oldAttrKeys.next())) {
					isDifferent = true;
					break;
				}
			}
		}

		return isDifferent;

	}

	/**
	 * 比较两相同元数据的所有属性是否相同,比较规则:判断新元数据的每一个属性,是否在旧元数据中存在,如不存在,则认为属性发生变化;如没变化,则再判断旧数据属性,
	 * 是否有不在新数据中的属性,如有则认为属性发生变化.
	 * 
	 * @param o
	 *            旧数据
	 * @return 如完全相同则返回false,否则返回true
	 */
	public boolean pkIsDifferent(MMMetadata o) {
		Iterator<String> attrKeys = attrs.keySet().iterator();
		boolean isDifferent = false;
		while (attrKeys.hasNext()) {
			String attrKey = attrKeys.next();
			if (!o.getAttrs().containsKey(attrKey)
					|| (o.getAttrs().containsKey(attrKey) && !attrs.get(attrKey).equals(o.getAttrs().get(attrKey)))) {
				// 1.key存在于新元数据中,但不存在于旧元数据中,需要设置该属性(包括key,value);2.key相同,但value不相同,此时同时需要设置该属性
				isDifferent = true;
				break;
			}
		}

		if (!isDifferent) {
			// 仍然相同,则遍历旧数据的属性,判断是否有不存在于新数据中的属性,如有,则认为两元数据属性不一致
			Iterator<String> oldAttrKeys = o.getAttrs().keySet().iterator();
			while (oldAttrKeys.hasNext()) {
				if (!attrs.containsKey(oldAttrKeys.next())) {
					isDifferent = true;
					break;
				}
			}
		}

		return isDifferent;

	}

	/**
	 * 比较两元数据是否相同:元模型ID相同;code相同;父结点相同,则认为两元数据相同;此比较也为元数据排序的依据
	 */
	public int compareTo(MMMetadata o) {
		// 1.先比较code
		int compareInt = code.compareTo(o.getCode());
		if (compareInt == 0) {
			// 2.然后比较元模型
			compareInt = classifierId.compareTo(o.getClassifierId());
			if (o.id == id) { return compareInt; }
			if (compareInt == 0) {
				if (parentMetadata == null || o.getParentMetadata() == null
						|| (parentMetadata.getId() == null && o.getParentMetadata().getId() == null)) { return compareInt; }
				compareInt = parentMetadata.compareTo(o.getParentMetadata());
			}
		}
		return compareInt;
	}

	public String getUid() {
		if (uid == null) {
			StringBuilder sb = new StringBuilder(classifierId);
			if (code != null) {
				sb.append(Constants.METADATA_DUPLICATE_SEPARATOR).append(code);
			}
			if (parentMetadata != null) {
				sb.append(Constants.METADATA_DUPLICATE_SEPARATOR).append(parentMetadata.getUid());
			}
			uid = sb.toString();
		}
		return uid;

	}

	public int hashCode() {

		if (hashCode == 0) {
			if (classifierId != null) {
				hashCode = classifierId.hashCode();
			}
			if (code != null) {
				hashCode += (hashCode << 4) + code.hashCode();
			}
			if (parentMetadata != null) {
				hashCode += (hashCode << 4) + parentMetadata.hashCode();
			}
		}
		return hashCode;
	}

	public int compareTo2(MMMetadata o) {
		// 比较父结点是否一样
		int compareInt = parentMetadata.getId().compareTo(o.getParentMetadata().getId());
		if (compareInt == 0) {
			// 如为相同父结点,则再比较本结点的code
			compareInt = code.compareTo(o.getCode());
		}
		return compareInt;
	}

	/**
	 * 对相同元数据的属性作比较 产生一个需要修改的元数据，它包含：元数据ID;该元数据所有添加，修改的属性放入属性增加列表中;删除的属性放入删除列表中
	 * 
	 * @param o
	 * @return
	 */
	public ModifyMetadata genDiffAttrs(MMMetaModel newMetaModel, MMMetadata o) {

		Iterator<String> keys = attrs.keySet().iterator();
		String key = null;
		String value = null;

		int size = attrs.size();
		/**
		 * 增加的属性
		 */
		Map<String, String> addAttrs = new HashMap<String, String>(size / 3);
		/**
		 * 修改的属性
		 */
		List<ModifyAttribute> changeAttrs = new ArrayList<ModifyAttribute>(size / 3);

		/**
		 * 不变的属性
		 */
		Map<String, String> changelessAttrs = new HashMap<String, String>(size);
		//（华润）如果为集采，需对属性进行判断，是否对该属性进行覆盖
		if(AdapterExtractorContext.getInstance().getIsDBExtract()){
			//获取所存储的CLASS数据
			Map<String,TemplateTitleCover> tmplTitle = AdapterExtractorContext.getInstance().getIsCover();
			TemplateTitleCover title = tmplTitle.get(o.getClassifierId());
			Map<String,String> attr = title.getAtt();
			//不管有没有属性，都设置不全量
			newMetaModel.setHaveChangelessAttr(true);
			for(Iterator<String> att = attr.keySet().iterator();att.hasNext();){
				//添加可变字段到newMetaModel中
				String name = att.next();
				//当状态值为0的时候，需进行覆盖
				if(attr.get(name)!=null&&attr.get(name).equals("0")){
					newMetaModel.addNeedUpdateAttr(name);
				}
			}
		}
		

		if (newMetaModel.isHaveChangelessAttr()) {
			// 有不需要变更的属性
			Set<String> needUpdateAttrs = newMetaModel.getNeedUpdateAttrs();
			while (keys.hasNext()) {
				key = keys.next();
				if (needUpdateAttrs.contains(key)) {
					// 该属性为需要更新的属性
					// 数据比较时也需要作截断,因为从库表中的数据获取过来时,已经作了截断
					String val = attrs.get(key) == null ? "" : attrs.get(key);
					String newValue = val;

					if (!o.getAttrs().containsKey(key)) {
						// 旧数据中不存在此key,则认为其需要添加
						if (!"".equals(newValue)) {
							// 为空的数据将不添加
							addAttrs.put(key, newValue);
						}

						continue;
					}

					value = o.getAttr(key);

					if (value.equals(newValue)) {
						// 相同的属性及属性值
						changelessAttrs.put(key, value);
					}
					else {
						// 修改的属性
						ModifyAttribute mAttr = new ModifyAttribute();
						mAttr.setKey(key);
						mAttr.setOldValue(value);
						mAttr.setNewValue(newValue);

						changeAttrs.add(mAttr);
					}
				}
				else {
					// 该属性为无需变更的属性,此时直接从旧数据中的属性中获取属性值即可(如果有的话)
					if (o.getAttrs().containsKey(key)) {
						changelessAttrs.put(key, o.getAttr(key));
					}

				}
			}
			// 发现对于有不需要更新的情况,以上还判断不全,需要补充对DB中的属性的判断
			for (Iterator<String> dbKeys = o.getAttrs().keySet().iterator(); dbKeys.hasNext();) {
				// 判断有没有DB中的属性值,是不需要变更的
				String dbKey = dbKeys.next();
				if (!needUpdateAttrs.contains(dbKey)) {
					// 在DB中的属性,不在需要变更的列表中,即表示该属性不需要变更
					changelessAttrs.put(dbKey, o.getAttr(dbKey));
				}
			}

		}
		else {
			// 没有不需要变更的属性,即所有的属性,如果有变更都需要变更
			while (keys.hasNext()) {
				key = keys.next();
				// 数据比较时也需要作截断,因为从库表中的数据获取过来时,已经作了截断
				String val = attrs.get(key) == null ? "" : attrs.get(key);
				String newValue = val;

				if (!o.getAttrs().containsKey(key)) {
					// 旧数据中不存在此key,则认为其需要添加
					if (!"".equals(newValue)) {
						// 为空的数据将不添加
						addAttrs.put(key, newValue);
					}

					continue;
				}

				value = o.getAttr(key);

				if (value.equals(newValue)) {
					// 相同的属性及属性值
					changelessAttrs.put(key, value);
				}
				else {
					// 修改的属性
					ModifyAttribute mAttr = new ModifyAttribute();
					mAttr.setKey(key);
					mAttr.setOldValue(value);
					mAttr.setNewValue(newValue);

					changeAttrs.add(mAttr);
				}
			}
		}

		// 获取增加的属性
		/**
		 * 删除的属性
		 */
		Map<String, String> delAttrs = getDeleteAttrs(newMetaModel, o);

		if (addAttrs.size() == 0 && delAttrs.size() == 0 && changeAttrs.size() == 0) {
			// 元数据没有发生变化
			return null;
		}

		Map<Operation, Map<String, String>> modifyAttrs = new HashMap<Operation, Map<String, String>>();
		if (addAttrs.size() > 0) {
			// 增加的属性
			modifyAttrs.put(Operation.CREATE, addAttrs);

		}
		if (delAttrs.size() > 0) {
			// 删除的属性
			modifyAttrs.put(Operation.DELETE, delAttrs);
		}
		if (changelessAttrs.size() > 0) {
			// 不变的属性
			modifyAttrs.put(Operation.CHANGELESS, changelessAttrs);
		}

		ModifyMetadata modifyMetadata = new ModifyMetadata();
		modifyMetadata.setId(o.getId());
		if (modifyAttrs.size() > 0) {
			modifyMetadata.setModifyAttrs(modifyAttrs);

		}
		if (changeAttrs.size() > 0) {
			// 修改的属性
			modifyMetadata.setMAttrs(changeAttrs);
		}
		if (newMetaModel.isHaveChangelessAttr()) {
			// 有该属性，则名称也为不变项
			modifyMetadata.setName(o.getName());
		}
		else {
			if(AdapterExtractorContext.getInstance().getIsDBExtract())
				modifyMetadata.setName(o.getName());
			else
				modifyMetadata.setName(name == null ? code : name);
		}
		modifyMetadata.setClassifierId(classifierId);
		modifyMetadata.setStartTime(o.startTime); // 别忘了时间
		return modifyMetadata;
	}

	private Map<String, String> getDeleteAttrs(MMMetaModel newMetaModel, MMMetadata o) {
		Iterator<String> ownerKeys = o.getAttrs().keySet().iterator();
		// 删除的元数据属性
		Map<String, String> delAttrs = new HashMap<String, String>(0);
		String ownerKey = null;

		if (newMetaModel.isHaveChangelessAttr()) {
			// 想来想去,这里就没有需要删除的属性了..是吧,故把这段注释掉
			// 有不需要变更的属性
			// while (ownerKeys.hasNext()) {
			// ownerKey = ownerKeys.next();
			// if (!attrs.containsKey(ownerKey) &&
			// newMetaModel.getNeedUpdateAttrs().contains(ownerKey)) {
			// // 3.比较元数据中不存在，但在被比较元数据中存在的key,将其放入添加列表中。
			// delAttrs.put(ownerKey, o.getAttr(ownerKey));
			// }
			// }
		}
		else {
			// 没有不需要变更的属性,即所有的属性,如果有变更都需要变更
			while (ownerKeys.hasNext()) {
				ownerKey = ownerKeys.next();
				if (!attrs.containsKey(ownerKey)) {
					// 3.比较元数据中不存在，但在被比较元数据中存在的key,将其放入添加列表中。
					delAttrs.put(ownerKey, o.getAttr(ownerKey));
				}
			}
		}

		return delAttrs;
	}

	public boolean isHasExist() {
		return hasExist;
	}

	public void setHasExist(boolean hasExist) {
		this.hasExist = hasExist;
	}

	public String getClassifierId() {
		return classifierId;
	}

	public void setClassifierId(String classifierId) {
		this.classifierId = classifierId;
	}

	public void print() {
		log.info("Metadata[id:" + id);
		log.info(",classifierId:" + this.classifierId);
		log.info(",name:" + this.name);
		log.info(",parentId:" + this.parentMetadata.id);
		if (attrs.size() <= 0) {
			log.info("]");
			return;
		}
		log.info(",Attrs[");
		Iterator<String> keys = attrs.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			log.info(",key:" + key + ",value:" + attrs.get(key));
		}
		log.info("]]");

	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String genNamespace() {
		if (namespace == null) {
			namespace = new StringBuilder(parentMetadata.genNamespace()).append("/").append(id).toString();
		}
		return namespace;
	}

	public void addChildMetadata(MMMetadata metadata) {
		children.add(metadata);
	}

	@Override
	public List<AbstractMetadata> getChildrenMetadatas() {

		return children;
	}

}
