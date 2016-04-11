package com.pactera.edg.am.metamanager.extractor.bo.mm;

import java.io.Serializable;

import com.pactera.edg.am.metamanager.extractor.util.Constants;

/**
 * 描述元数据间的依赖关系
 * 
 * @author qhchen
 * @version 1.0 2010-05-04
 */
public class MMDDependency implements Comparable<MMDDependency>, Serializable {
	private static final long serialVersionUID = 343446546576578L;

	private String uid;

	/**
	 * 依赖元数据对象
	 */
	private MMMetadata ownerMetadata;

	/**
	 * 被依赖关系数据对象
	 */
	private MMMetadata valueMetadata;

	private String ownerRole;

	private String valueRole;

	private String code;

	private int hashCode;

	private String description;

	public MMMetadata getOwnerMetadata() {
		return ownerMetadata;
	}

	public void setOwnerMetadata(MMMetadata ownerMetadata) {
		this.ownerMetadata = ownerMetadata;
	}

	public MMMetadata getValueMetadata() {
		return valueMetadata;
	}

	public void setValueMetadata(MMMetadata valueMetadata) {
		this.valueMetadata = valueMetadata;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getOwnerRole() {
		return ownerRole;
	}

	public void setOwnerRole(String ownerRole) {
		this.ownerRole = ownerRole;
	}

	public String getValueRole() {
		return valueRole;
	}

	public void setValueRole(String valueRole) {
		this.valueRole = valueRole;
	}

	public int compareTo(MMDDependency o) {
		int compareInt = ownerMetadata.getId().compareTo(o.getOwnerMetadata().getId());
		if (compareInt == 0) {
			compareInt = valueMetadata.getId().compareTo(o.getValueMetadata().getId());
			if (compareInt == 0) {
				compareInt = code.compareTo(o.getCode());
			}
		}
		return compareInt;
	}

	public boolean equals(Object obj) {
		return (obj instanceof MMDDependency) ? compareTo((MMDDependency) obj) == 0 : false;
	}

	public String getUid() {
		if (uid == null) {
			StringBuilder sb = new StringBuilder(ownerMetadata.getUid());
			sb.append(Constants.METADATA_DUPLICATE_SEPARATOR).append(valueMetadata.getUid());
			if (ownerRole != null) {
				sb.append(Constants.METADATA_DUPLICATE_SEPARATOR).append(ownerRole);
			}
			if (valueRole != null) {
				sb.append(Constants.METADATA_DUPLICATE_SEPARATOR).append(valueRole);
			}
			uid = sb.toString();
		}
		return uid;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {

		if (hashCode == 0) {
			hashCode = ownerMetadata.hashCode();
			hashCode += (hashCode << 4) + valueMetadata.hashCode();
			if (ownerRole != null) {
				hashCode += (hashCode << 4) + ownerRole.hashCode();
			}
			if (valueRole != null) {
				hashCode += (hashCode << 4) + valueRole.hashCode();
			}
		}
		return hashCode;
	}

	public String toString() {
		return "owner cls:" + this.getOwnerMetadata().getClassifierId() + ",owner:" + this.getOwnerMetadata().getCode()
				+ ",name:" + this.getOwnerMetadata().getName() + ",value cls:"
				+ this.getValueMetadata().getClassifierId() + ",value:" + this.getValueMetadata().getCode() + ",name:"
				+ this.getValueMetadata().getName();
	}
}
