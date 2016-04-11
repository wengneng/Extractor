package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

import java.io.Serializable;

/**
 * 元数据关系
 * 
 * @author user
 * @version 1.0
 * 
 */
public class DRelationship implements Comparable<DRelationship>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 23435789798754308L;

	// 关系的源着一端元数据
	private Metadata ownerMetadata;

	// 关系的目标一端元数据
	private Metadata valueMetadata;

	// 关系名
	private String name;

	private String description;

	// 关系类型:1为composition合成，2为dependency依赖，3为association非合成的关联关系
	private byte type;

	// 关系ID
	private long uuid;

	// 所属的元模型的关系
	private MRelationship mRelationship;

	public long getUuid() {
		return uuid;
	}

	public void setUuid(long uuid) {
		this.uuid = uuid;
	}

	public MRelationship getMRelationship() {
		return mRelationship;
	}

	public void setMRelationship(MRelationship relationship) {
		mRelationship = relationship;
	}

	public Metadata getOwnerMetadata() {
		return ownerMetadata;
	}

	public void setOwnerMetadata(Metadata ownerMetadata) {
		this.ownerMetadata = ownerMetadata;
	}

	public Metadata getValueMetadata() {
		return valueMetadata;
	}

	public void setValueMetadata(Metadata valueMetadata) {
		this.valueMetadata = valueMetadata;
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

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public int compareTo(DRelationship o) {
		// 先判断OWNER元数据的顺序
		int compareInt = ownerMetadata.compareTo(o.getValueMetadata());
		if (compareInt == 0) {
			// 如果OWNER元数据相同，则判断VALUE元数据的顺序
			compareInt = valueMetadata.compareTo(o.getValueMetadata());
			if (compareInt == 0) {
				// 如果两者都相同，再判断关系类型
				compareInt = type - o.getType();
			}
		}

		return compareInt;
	}

}
