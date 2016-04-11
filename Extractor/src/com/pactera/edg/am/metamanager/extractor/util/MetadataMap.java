/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.map.AbstractHashedMap;

import com.pactera.edg.am.metamanager.app.recordextra.vo.TRecordConfigFull.TRecordInherit;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 方便存取元数据的Map
 * @author fbchen
 * @version 2.2 2011-08-12
 */
public class MetadataMap extends AbstractHashedMap {

	/**
	 * 创建Map
	 */
	public MetadataMap() {
		super(16, 0.75F, 12);
	}

	/**
	 * 创建Map
	 * @param initialCapacity
	 */
	public MetadataMap(int initialCapacity) {
		super(initialCapacity);
	}

	/**
	 * 获取引用的唯一键值:ClassifierId, InstanceId
	 * @param d MMMetadata 元数据
	 * @return MdKey
	 */
	public static MdKey getMetadataKey(MMMetadata d) {
		return getMetadataKey(d.getClassifierId(), d.getUid());
	}
	
	/**
	 * 获取引用的唯一键值:ClassifierId, InstanceId
	 * @param classifierId 元模型
	 * @param uid 唯一标识
	 * @return MdKey
	 */
	public static MdKey getMetadataKey(String classifierId, String uid) {
		return new MdKey(classifierId, uid);
	}
	
	/**
	 * 判断是否存在对应的元数据Key
	 * @param classifierId 元模型ID
	 * @param uid 元数据UID
	 * @return boolean
	 */
	public boolean containsKey(String classifierId, String uid) {
		return super.containsKey(getMetadataKey(classifierId, uid));
	}
	
	/**
	 * 根据基类判断是否存在对应的子类的元数据Key。如基类“Classifier”包含的子类有“Table”、“View”等。
	 * @param baseKey 基类元数据的key
	 * @param inherits 继承关系列表
	 * @return boolean
	 */
	public boolean containsKeyInherit(MdKey baseKey, List<TRecordInherit> inherits) {
		if (this.containsKey(baseKey)) {
			return true;
		}
		if (inherits != null && !inherits.isEmpty()) {
			for (Iterator<TRecordInherit> it = inherits.iterator(); it.hasNext();) {
				TRecordInherit inherit = it.next();
				if (inherit.getBaseClassifierId().equals(baseKey.classifierId) &&
						this.containsKey(inherit.getChildClassifierId(), baseKey.uid)) {
					return true;
				}
			}
		}
		return false;
	}
	
	public List<MMMetadata> getByInherit(MdKey baseKey, List<TRecordInherit> inherits) {
		List<MMMetadata> mds = new ArrayList<MMMetadata>();
		
		MMMetadata d1 = this.get(baseKey);
		if (d1 != null) {
			mds.add(d1);
		}
		
		if (inherits != null && !inherits.isEmpty()) {
			for (Iterator<TRecordInherit> it = inherits.iterator(); it.hasNext();) {
				TRecordInherit inherit = it.next();
				if (inherit.getBaseClassifierId().equals(baseKey.classifierId) &&
						this.containsKey(inherit.getChildClassifierId(), baseKey.uid)) {
					MdKey key = getMetadataKey(inherit.getChildClassifierId(), baseKey.uid);
					mds.add(this.get(key));
				}
			}
		}
		return mds;
	}
	

	public MMMetadata get(MdKey key) {
		return (MMMetadata) super.get(key);
	}

	@Override
	protected Object convertKey(Object key) {
		if(key != null) {
			return key;
		}
        else {
        	return AbstractHashedMap.NULL;
        }
	}

	/**
	 * 元数据的键
	 * @author fbchen
	 */
	public static final class MdKey implements Serializable {
		private static final long serialVersionUID = 1L;
		String classifierId;
		String uid;

		public String getClassifierId() {
			return classifierId;
		}

		public void setClassifierId(String classifierId) {
			this.classifierId = classifierId;
		}

		public String getUid() {
			return uid;
		}

		public void setUid(String uid) {
			this.uid = uid;
		}

		public MdKey() {
			super();
		}

		public MdKey(String classifierId, String uid) {
			super();
			this.classifierId = classifierId;
			this.uid = uid;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((classifierId == null) ? 0 : classifierId.hashCode());
			result = prime * result + ((uid == null) ? 0 : uid.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}
			MdKey other = (MdKey) obj;
			if (classifierId == null) {
				if (other.classifierId != null) {
					return false;
				}
			} else if (!classifierId.equals(other.classifierId)) {
				return false;
			}
			if (uid == null) {
				if (other.uid != null) {
					return false;
				}
			} else if (!uid.equals(other.uid)) {
				return false;
			}
			return true;
		}
	}

}
