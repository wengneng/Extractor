package com.pactera.edg.am.metamanager.extractor.bo.mappingload;

import java.io.Serializable;

/**
 * 元数据关系操作的类型
 * 
 */
public enum Operation implements Serializable{
	CREATE, MODIFY, DELETE, CHANGELESS
}
