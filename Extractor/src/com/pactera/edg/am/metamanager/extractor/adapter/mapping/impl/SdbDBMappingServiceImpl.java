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

package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.util.List;

import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Partition;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;

/**
 * 深发展特有功能:实现对表分区的转换
 * 
 * @author user
 * @version 1.0 Date: Nov 5, 2009
 * 
 */
public class SdbDBMappingServiceImpl extends DBMappingServiceImpl {

	protected void genPartitions(MMMetadata parentMetadata, Table table, MMMetaModel partitionMetaModel) {
		List<Partition> partitions = table.getPartitions();
		if (partitions.size() == 0) { return; }
		for (Partition partition : partitions) {
			MMMetadata metadata = new MMMetadata();
			metadata.setCode(partition.getName());
			metadata.setAttrs(partition.getAttrs());
			metadata.setParentMetadata(parentMetadata);
			metadata.setClassifierId(partitionMetaModel.getCode());

			// addPartitionDependency(metadata, partition.getTableName(), )
			partitionMetaModel.addMetadata(metadata);
		}
		partitionMetaModel.setHasMetadata(true);
	}
}
