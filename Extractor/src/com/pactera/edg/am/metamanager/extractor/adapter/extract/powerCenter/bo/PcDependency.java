package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo;

public class PcDependency {
	private String fromId;
	private String toId;
	private String fromRole;
	private String toRole;

	public PcDependency(String fromId,String fromRole, String toId,String toRole) {
		this.fromId = fromId;
		this.toId = toId;
		this.fromRole=fromRole;
		this.toRole=toRole;
	}

	public String getFromId() {
		return fromId;
	}

	public String getToId() {
		return toId;
	}

	public String getFromRole() {
		return fromRole;
	}

	public String getToRole() {
		return toRole;
	}
}
