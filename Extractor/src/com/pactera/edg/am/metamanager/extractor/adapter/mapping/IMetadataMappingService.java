package com.pactera.edg.am.metamanager.extractor.adapter.mapping;

import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;

/**
 * 转换器接口
 *
 * @author user
 * @version 1.0  Date: Jul 7, 2009
 *
 */
public interface IMetadataMappingService {
	
	final String SPRING_NAME = "iMetadataMappingService";
	
	/**
	 * 负责将数据源的数据对象转换为公共对象
	 */
	void metadataMapping(AppendMetadata aMetadata) throws Exception;

}
