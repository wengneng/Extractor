package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.parse;

import java.util.List;
import java.util.Map;

import org.jdom.Document;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public interface IRefDataSource {
	Map<String, List<?>> loadDataSource(Document xmlDoc);
}
