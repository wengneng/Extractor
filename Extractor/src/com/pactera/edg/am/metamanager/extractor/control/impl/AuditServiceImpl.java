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

package com.pactera.edg.am.metamanager.extractor.control.impl;

import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.audit.IAuditMetadataService;
import com.pactera.edg.am.metamanager.extractor.bo.mappingload.Operation;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.control.IAuditService;
import com.pactera.edg.am.metamanager.extractor.load.IMetadataLoaderService;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.ExtractorContextLoader;

/**
 * 审核入库控制实现类
 * 
 * @author user
 * @version 1.0 Date: Oct 12, 2009
 * 
 */
public class AuditServiceImpl implements IAuditService {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.control.AuditService#operate()
	 */
	public boolean operate() {
		try {
			AdapterExtractorContext.getInstance().setIClassifier(
					ExtractorContextLoader.getMMRmiServer());
			
			IAuditMetadataService auditMetadataService = (IAuditMetadataService) ExtractorContextLoader
					.getBean(IAuditMetadataService.SPRING_NAME);
			Map<Operation, AppendMetadata> aMetadata = auditMetadataService.genAuditAMetadata();

			IMetadataLoaderService loader = (IMetadataLoaderService) ExtractorContextLoader
					.getBean(IMetadataLoaderService.SPRING_NAME);
			try{
			loader.auditLoadMetadata(aMetadata);
			}catch (Exception e){
				//临时屏蔽因为主键重复而报错的数据.
			}
			return true;

		}
		catch (Throwable t) {
			t.printStackTrace();
			return false;
		}
	}

}
