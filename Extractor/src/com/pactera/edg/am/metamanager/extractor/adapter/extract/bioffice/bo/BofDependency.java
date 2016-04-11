package com.pactera.edg.am.metamanager.extractor.adapter.extract.bioffice.bo;

public class BofDependency {
	private String fromId;
	private String toId;

	public BofDependency(String fromId, String toId) {
		this.fromId = fromId;
		this.toId = toId;
	}

	public String getFromId() {
		return fromId;
	}

	public void setFromId(String fromId) {
		this.fromId = fromId;
	}

	public String getToId() {
		return toId;
	}

	public void setToId(String toId) {
		this.toId = toId;
	}
}
