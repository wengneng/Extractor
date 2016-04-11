package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 映射器的基类
 * 
 * @author user
 * @version 1.0
 * 
 */
public class BaseMappingServiceImpl {

	public MMMetadata createMetadata(MMMetadata parentMetadata, MMMetaModel cntMetaModel, String metadataCode,
			String metadataName) {
		MMMetadata metadata = new MMMetadata();
		metadata.setCode(metadataCode);
		if (metadataName != null && !metadataName.equals("")) {
			metadata.setName(metadataName);
		}
		metadata.setParentMetadata(parentMetadata);
		parentMetadata.addChildMetadata(metadata);
		metadata.setClassifierId(cntMetaModel.getCode());

		cntMetaModel.addMetadata(metadata);
		cntMetaModel.setHasMetadata(true);

		return metadata;
	}
}
