package com.pactera.edg.am.metamanager.extractor.bo.composite;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 元数据存储对象，只存储非根结点，且父结点不存在于存储库中的结点 使用说明：应为RootMetadata 组合
 * Metadata,Metadata可以自组合。其中RootMetadata存储根结点，或父结点已经存在于存储库中的数据，
 * Metadata只存储非根结点，且父结点不存在于存储库中的结点
 * 
 * 注意：采用这种存储模式，则不能约束模型组合的正确性，需通过程序检测其正确与否，注意不要出现环形数据
 * 
 * @author user
 * 
 */
public class CompMetadata implements Comparable<CompMetadata> {

	private Log log = LogFactory.getLog(CompRootMetadata.class);

	private Map<MetaModel, List<CompMetadata>> metadatas;

	private String id;

	// 元数据名
	private String name;

	private String description;

	// 元数据属性key,value
	private Map<String, String> attrs;

	// 元数据父结点
	private CompMetadata parentMetadata;

	public String getId() {
		return id;
	}

	public CompMetadata getParentMetadata() {
		return parentMetadata;
	}

	public void setParentMetadata(CompMetadata parentMetadata) {
		this.parentMetadata = parentMetadata;
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

	public String getAttr(String key) {
		return attrs.get(key);
	}

	public void setAttrs(Map<String, String> attrs) {
		this.attrs = attrs;
	}

	public void print() {

	}

	public static void main(String[] args) {
		CompRootMetadata rm = new CompRootMetadata();
		rm.setName("sdfkj");
		rm.setParentPath("kkkkkkkkkkkkkkkkk");
		CompRootMetadata rm1 = new CompRootMetadata();
		rm1.setName("33333");
		rm1.setParentPath("22222222");

		DDependency dd = new DDependency();
		dd.setValueMetadata(rm1);
		dd.setOwnerMetadata(rm);
		dd.getValueMetadata();
	}

	public Map<MetaModel, List<CompMetadata>> getMetadatas() {
		return metadatas;
	}

	public void setMetadatas(Map<MetaModel, List<CompMetadata>> metadatas) {
		this.metadatas = metadatas;
	}

	public void setId(String id) {
		this.id = id;
	}

	/**
	 * 用于元数据排序
	 */
	public int compareTo(CompMetadata o) {
		return name.hashCode() - o.getName().hashCode();
	}

	public Map<String, String> compareAttrTo(CompMetadata o) {
		Iterator<String> keys = attrs.keySet().iterator();
		String key = null;
		String value = null;
		/**
		 * 增加的属性，同时将修改的属性也放入此中
		 */
		Map<String, String> addAttrs = new HashMap<String, String>();

		while (keys.hasNext()) {
			key = keys.next();
			if (!o.getAttrs().containsKey(key)) {
				// 新数据中不存在此key,则认为其需要删除s
				continue;
			}

			value = o.getAttr(key);

			if (!value.equals(attrs.get(key))) {
				// 修改的属性
				addAttrs.put(key, value);
			}
		}

		// 获取增加的属性
		addAttrs.putAll(getCreateAttrs(o));

		if (addAttrs.size() > 0) { return addAttrs; }
		return null;
	}

	/**
	 * 比较新旧数据，产生新增的元数据属性
	 * 
	 * @param o
	 *            CompMetadata
	 * @return
	 */
	private Map<String, String> getCreateAttrs(CompMetadata o) {
		Iterator<String> ownerKeys = o.getAttrs().keySet().iterator();
		// 添加的元数据属性
		Map<String, String> addAttrs = new HashMap<String, String>();
		String ownerKey = null;
		while (ownerKeys.hasNext()) {
			ownerKey = ownerKeys.next();
			if (!attrs.containsKey(ownerKey)) {
				// 3.比较元数据中不存在，但在被比较元数据中存在的key,将其放入添加列表中。
				addAttrs.put(ownerKey, o.getAttr(ownerKey));
			}
		}
		return addAttrs;
	}
}
