package com.pactera.edg.am.metamanager.extractor.bo.composite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 元数据存储对象，只存储根结点，或父结点存在于存储库中的结点
 * 
 * @author user
 * 
 */
public class CompRootMetadata extends CompMetadata {

	private Log log = LogFactory.getLog(CompRootMetadata.class);

	private String parentPath;
	

	public String getParentPath() {
		return parentPath;
	}

	public void setParentPath(String parentPath) {
		this.parentPath = parentPath;
	}

	public void print() {
		if (parentPath != null && !parentPath.equals("")) {
			log.info("\n父结点[" + getParentPath() + "],");
		}
		super.print();
	}

}
