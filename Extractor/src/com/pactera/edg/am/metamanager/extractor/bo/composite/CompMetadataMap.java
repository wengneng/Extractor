package com.pactera.edg.am.metamanager.extractor.bo.composite;

import java.util.List;
import java.util.Map;

public class CompMetadataMap {

	private Map<MetaModel, List<CompRootMetadata>> metadatas;
	
	private Map<MDependency, List<DDependency>> dDependencys;

	public Map<MetaModel, List<CompRootMetadata>> getMetadatas() {
		return metadatas;
	}

	public void setMetadatas(Map<MetaModel, List<CompRootMetadata>> metadatas) {
		this.metadatas = metadatas;
	}

	public Map<MDependency, List<DDependency>> getDDependencys() {
		return dDependencys;
	}

	public void setDDependencys(Map<MDependency, List<DDependency>> dependencys) {
		dDependencys = dependencys;
	}
	
	
}
