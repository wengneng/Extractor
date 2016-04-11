package com.pactera.edg.am.metamanager.extractor.increment;

/**
 * 持久化数据比较:1.先将新数据持久化到磁盘;2.从库表中读取旧数据;3.分批从磁盘中读取新数据,并与旧数据在内存中作比较,从而产生需要添加,需要修改的元数据
 * 4.持久化旧数据到磁盘,同时持久化需要添加,修改的元数据到磁盘;5.分批从磁盘中读取旧数据,并与新数据在内存中作比较,从而产生需要删除的元数据;
 * 6.依赖关系的比较同元数据比较方式进行.从而产生增量后的元数据及依赖关系
 * 
 * @author user
 * @version 1.0
 * 
 */
public class MetadataMapPersitent {

}
