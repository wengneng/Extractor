/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, BeiJing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */


package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerdesigner;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlThirdAdapter;

/**
 * PowerDesigner
 *
 * @author hqd
 * @version 1.0  Date: Sep 25, 2009
 */
public class PowerDesignerXsltTransformer extends AbstractXmlThirdAdapter {

	@Override
	protected String getInValidType() {
		return null;
	}

	@Override
	protected String getInXsdOrDtd() {
		return null;
	}

	@Override
	protected String getToolName() {
		return "powerDesigner";
	}

	@Override
	protected String getVersion() {
		return "12";
	}

	@Override
	protected String getXslt() {
		return "classpath:/extractor/adapter/xsl/powerDesigner.xsl";
	}

	@Override
	protected void clear() {
		
	}

}
