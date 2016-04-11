package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel.test;

import java.util.*;

public interface TestMetaModle {
	public String getClass(String className) throws Exception;
	
	/**
	 * 
	 * @param templetId 模板ID
	 * @return
	 * @throws Exception
	 */
	public HashMap<String,String> getClassTree(String templetId) throws Exception;
}
