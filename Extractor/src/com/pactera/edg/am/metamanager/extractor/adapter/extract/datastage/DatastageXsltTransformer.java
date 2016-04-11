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

package com.pactera.edg.am.metamanager.extractor.adapter.extract.datastage;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlThirdAdapter;

/**
 * (简单概要描述此类完成的功能)
 * 
 * @author user
 * @version 1.0 Date: Sep 18, 2009
 * 
 */
public class DatastageXsltTransformer extends AbstractXmlThirdAdapter {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlAdapter#getInValidType()
	 */
	@Override
	protected String getInValidType() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlAdapter#getInXsdOrDtd()
	 */
	@Override
	protected String getInXsdOrDtd() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlAdapter#getToolName()
	 */
	@Override
	protected String getToolName() {
		return "Datastage";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlAdapter#getVersion()
	 */
	@Override
	protected String getVersion() {
		return "7.5";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.AbstractXmlAdapter#getXslt()
	 */
	@Override
	protected String getXslt() {
		return "com/pactera.edg.am/metamanager/extractor/adapter/extract/datastage/Datastage.xsl";
	}

	@Override
	protected void clear() {
		// TODO Auto-generated method stub
		
	}

}
