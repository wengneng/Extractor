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

package com.pactera.edg.am.metamanager.extractor.adapter.mapping.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.app.bo.TRecordClassifier;
import com.pactera.edg.am.metamanager.app.bo.TRecordFeature;
import com.pactera.edg.am.metamanager.app.bo.TRecordRelationship;
import com.pactera.edg.am.metamanager.app.recordextra.vo.TRecordConfigFull;
import com.pactera.edg.am.metamanager.core.common.connect.ConnectionVisitor;
import com.pactera.edg.am.metamanager.core.common.connect.ResultSetGetter;
import com.pactera.edg.am.metamanager.core.dao.session.SessionInterceptor;
import com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService;
import com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMDDependency;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetaModel;
import com.pactera.edg.am.metamanager.extractor.bo.mm.MMMetadata;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.MetadataMap;
import com.pactera.edg.am.metamanager.extractor.util.MetadataMap.MdKey;

/**
 * 数据库表记录采集适配器+转换器。
 * 存在问题：假设存在自组合的关系，是否出现死循环。
 *
 * @author fbchen
 * @version 1.0  Date: 2010-1-15 下午03:39:18
 */
public class RecordExtractMappingServiceImpl extends BaseMappingServiceImpl
			implements IMetadataMappingService {
	private static Log log = LogFactory.getLog(RecordExtractMappingServiceImpl.class);

	// 每次查询N条
	public static final int SIZE = 5000;
	
	// 采集库连接访问器
	private ConnectionVisitor connectionVisitor;
	
	protected ConnectionVisitor getConnectionVisitor() {
		return connectionVisitor;
	}

	public void setConnectionVisitor(ConnectionVisitor connectionVisitor) {
		this.connectionVisitor = connectionVisitor;
	}
	
	// 指向所有的元数据，其中KEY为Classifier+|+InstanceId，VALUE为Instance
	private MetadataMap mdReferences = new MetadataMap();
	
	// 依赖关系：Key为依赖关系TRecordRelationship，Value为MultiMap:(依赖端uid-->被依赖端uid)
	private Map<TRecordRelationship,MultiMap> depReferences = new HashMap<TRecordRelationship,MultiMap>();

	/**
	 * TRecordConfigFull
	 */
	private TRecordConfigFull cfg = null;
	

	/**
	 * 通过RMI查询映射配置信息，有可能抛出异常
	 * @return TRecordConfigFull
	 */
	private TRecordConfigFull getTRecordConfigFull() {
		AdapterExtractorContext context = AdapterExtractorContext.getInstance();
		String datasourceId = context.getDatasourceId();
		return context.getIClassifier().getRecordConfigFullBy(datasourceId);
	}
	
	/* (non-Javadoc)
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.mapping.IMetadataMappingService
	 * #metadataMapping(com.pactera.edg.am.metamanager.extractor.bo.mm.AppendMetadata)
	 */
	public void metadataMapping(AppendMetadata metadata) throws Exception {
		SessionInterceptor si = new SessionInterceptor();
		si.setSessionFactory(connectionVisitor.getSessionFactory());
		try {
			// 开始打开Session
			si.beginIntercept();
			
			// 通过rmi获取映射配置
			this.cfg = this.getTRecordConfigFull();
			List<TRecordClassifier> tops = cfg.getTopConfigedClassifier();
			
			// 逐层读取元数据，放入结果集
			for (int i=0; i<tops.size(); i++) {
				MMMetaModel childmm = this.extractData(metadata.getMetaModel(),
						metadata.getMetadata(), cfg, tops.get(i));
				metadata.getChildMetaModels().add(childmm);
			}
			
			// 读取元数据的依赖关系，放入结果集
			metadata.setDDependencies(this.extractDependency(cfg));
			
			// 清空大量缓存的数据
			this.mdReferences.clear();
			this.depReferences.clear();
			
		} finally {
			// 最后关闭数据库连接
			si.endIntercept(); //关闭Session
			connectionVisitor.destroy();
		}
	}

	/**
	 * 抽取元数据。按元模型的组合层次逐层读取元数据，
	 * 同时按照组合关系查询子元数据；最后采集依赖关系，并从已有的数据中建立关系。
	 * @param parentMetamodel 上级元模型
	 * @param parentMetadata 上级元数据
	 * @param cfg 映射配置
	 * @param cls 元模型映射
	 * @return MMMetaModel，其中包含元数据、子元模型（以及子元数据）
	 * @throws SQLException 查询目标库异常
	 */
	protected MMMetaModel extractData(MMMetaModel parentMetamodel, MMMetadata parentMetadata, 
			TRecordConfigFull cfg, TRecordClassifier cls) throws SQLException {
		MMMetaModel mm = this.getMetamodel(parentMetamodel, cls.getClassifier());
		
		// 依赖关系准备
		List<TRecordRelationship> depList = cfg.findDependencyByToClassifier(cls.getClassifier());
		
		if (log.isInfoEnabled()) {
			StringBuffer sb = new StringBuffer();
			sb.append("查询元数据：Classifier=").append(cls.getCpath());
			sb.append(" IdRef=").append(cls.getIdRef());
			sb.append(" InstanceCodeRef=").append(cls.getInstanceCodeRef());
			sb.append(" InstanceNameRef=").append(cls.getInstanceNameRef());
			sb.append(" SQL=").append(cls.getSqlScript());
			log.info(sb.toString());
		}
		ResultSetGetter rs = null;
		Page page = new Page(0, SIZE);
		while (true) {
			rs = connectionVisitor.query(cls.getSqlScript(), page.startIndex, page.pageSize);
			if (log.isInfoEnabled()) {
				log.info("第" + (page.startIndex/page.pageSize + 1) + "次查询返回记录：" + (rs.getRowCount()) + "条");
			}
			for (int i = 0; i < rs.getRowCount(); i++) {
				Map<String, Object> row = rs.getRow(i);
				List<TRecordFeature> fs = cfg.findClassFeature(cls.getCpath());
				MMMetadata d = this.generateMetadata(parentMetadata, row, cls, fs);
				MdKey mdKey = MetadataMap.getMetadataKey(d);
				if (!this.mdReferences.containsKey(mdKey)) {//考虑查询的结果集中可能存在重复的记录，只取一条
					this.mdReferences.put(mdKey, d); //缓存
					mm.addMetadata(d);
					mm.setHasMetadata(true);
				}
				
				// 不查询关系表的依赖关系在此准备
				this.prepareDependencyRelation(row,depList);
			}
			
			if (rs.getRowCount() < SIZE) { //已经达到最后一页
				break;
			}
			page.next();
		}
		
		// 如果没有查询到任何数据，就此结束
		if (mm.getMetadatas().isEmpty()) {
			return mm;
		}
		// 查询组合关系和子元数据
		List<TRecordRelationship> comList = cfg.findClassComposition(cls.getCpath());
		for (int i=0; i<comList.size(); i++) {
			TRecordRelationship comRel = comList.get(i);
			this.extractChildData(mm, cfg, comRel);
		}
		
		return mm;
	}
	
	/**
	 * 按照组合关系查询子元数据。先查询组合关系，然后存入双向引用Map（fromKey&lt;-&gt;toKey），
	 * @param parentMetamodel 上级元模型
	 * @param cfg 映射配置
	 * @param comRel 组合关系
	 * @throws SQLException 数据库异常
	 */
	protected void extractChildData(MMMetaModel parentMetamodel, TRecordConfigFull cfg,
			TRecordRelationship comRel) throws SQLException {
		TRecordClassifier cls = cfg.findChildClassifier(comRel.getCpath(),
				comRel.getToClassifier());
		if (cls == null) {
			log.warn("找不到被组合元模型：" + comRel.getCpath()+"/"+comRel.getToClassifier());
			return ;
		}
		MMMetaModel mm = this.getMetamodel(parentMetamodel, cls.getClassifier());
		
		// 查询组合关系（Composition）:其中KEY为子元数据的Key，VALUE为父元数据的Key。
		Map<MdKey, MdKey> map = null;
		if (comRel.useSql()) {
			map = this.queryCompositionRelation(comRel); //固有的组合关系
			if (map.isEmpty()) { //组合关系不存在
				return ;
			}
		}
		
		// 依赖关系准备（dependency prepare）
		List<TRecordRelationship> depList = cfg.findDependencyByToClassifier(cls.getClassifier());
		
		// 查询被组合端（子元数据）
		if (log.isInfoEnabled()) {
			StringBuffer sb = new StringBuffer();
			sb.append("查询元数据：Classifier=").append(cls.getCpath());
			sb.append(" IdRef=").append(cls.getIdRef());
			sb.append(" InstanceCodeRef=").append(cls.getInstanceCodeRef());
			sb.append(" InstanceNameRef=").append(cls.getInstanceNameRef());
			sb.append(" SQL=").append(cls.getSqlScript());
			log.info(sb.toString());
		}
		ResultSetGetter rs = null;
		Page page = new Page(0, SIZE);
		while (true) {
			rs = connectionVisitor.query(cls.getSqlScript(), page.startIndex, page.pageSize);
			if (log.isInfoEnabled()) {
				log.info("第" + (page.startIndex/page.pageSize + 1) + "次查询返回记录：" + (rs.getRowCount()) + "条");
			}
			for (int i = 0; i < rs.getRowCount(); i++) {
				Map<String, Object> row = rs.getRow(i);
				String uid = extractValue(row, cls.getIdRef());
				MdKey childKey = MetadataMap.getMetadataKey(cls.getClassifier(), uid);
				MdKey parentKey = this.getParentMetadataKey(row, comRel, map, childKey); //上级元数据的键值
				if (parentKey == null) {
					continue;
				}
				
				// 从内存中提取组合端的元数据（父元数据）
				MMMetadata parentMetadata = this.mdReferences.get(parentKey);
				if (parentMetadata == null) {
					//log.warn("找不到组合元数据：" + comRel.getCpath());
					continue;
				}
				
				List<TRecordFeature> fs = cfg.findClassFeature(cls.getCpath()); //属性映射
				MMMetadata d = this.generateMetadata(parentMetadata, row, cls, fs);
				MdKey mdKey = MetadataMap.getMetadataKey(d);
				if (!this.mdReferences.containsKey(mdKey)) {//考虑查询的结果集中可能存在重复的记录，只取一条
					this.mdReferences.put(mdKey, d); //缓存
					mm.addMetadata(d);
					mm.setHasMetadata(true);
				}
				
				// 不查询关系表的依赖关系在此准备
				this.prepareDependencyRelation(row,depList);
			}
			
			if (rs.getRowCount() < SIZE) { //已经达到最后一页
				break;
			}
			page.next();
		}
		
		// 如果没有查询到任何数据，就此结束
		if (mm.getMetadatas().isEmpty()) {
			return ;
		}
		// 查询组合关系和子元数据
		List<TRecordRelationship> comList = cfg.findClassComposition(cls.getCpath());
		for (int i=0; i<comList.size(); i++) {
			TRecordRelationship childComRel = comList.get(i);
			this.extractChildData(mm, cfg, childComRel); //迭代往下
		}
		
	}

	/**
	 * 遍历所有的依赖关系，若要查询关系表获得关联数据则先读取关联表数据；<br>
	 * 然后根据内存depReferences中的依赖映射，生成处理器需要的关联结果以返回。
	 * @param cfg 映射配置
	 * @return 关联列表
	 * @throws SQLException 查询数据库异常
	 */
	protected List<MMDDependency> extractDependency(TRecordConfigFull cfg) throws SQLException {
		// 获取所有的依赖关系
		List<TRecordRelationship> depList = cfg.findAllDependency();
		for (int i = 0; i < depList.size(); i++) {
			if (depList.get(i).useSql()) {
				this.queryDependencyRelation(depList.get(i));
			}
		}
		
		// 组装依赖关系，以交付给上下文 (由于依赖关系的类可以是基类，因此匹配搜索两端的元数据要特殊处理)
		List<MMDDependency> result = new ArrayList<MMDDependency>();
		for (Iterator<TRecordRelationship> depIt = this.depReferences.keySet().iterator(); depIt.hasNext();) {
			TRecordRelationship depc = depIt.next();
			MultiMap map = this.depReferences.get(depc);
			for (Iterator<?> fromIt = map.keySet().iterator(); fromIt.hasNext();) {
				String fromId = (String) fromIt.next();
				Collection<?> toIds = (Collection<?>) map.get(fromId); //MultiMap返回的是多个值
				if (toIds == null || toIds.isEmpty()) {
					continue;
				}
				MdKey fromKey = MetadataMap.getMetadataKey(depc.getFromClassifier(), fromId);
				List<MMMetadata> fromMetadatas = this.mdReferences.getByInherit(fromKey, cfg.getInherits());
				if (fromMetadatas == null || fromMetadatas.isEmpty()) {
					continue;
				}
				for (Iterator<?> toIt = toIds.iterator(); toIt.hasNext(); ){
					String toId = (String) toIt.next();
					MdKey toKey = MetadataMap.getMetadataKey(depc.getToClassifier(), toId);
					List<MMMetadata> toMetadatas = this.mdReferences.getByInherit(toKey, cfg.getInherits());
					if (toMetadatas == null || toMetadatas.isEmpty()) {
						continue;
					}
					
					for (Iterator<MMMetadata> it1 = fromMetadatas.iterator(); it1.hasNext();) {
						MMMetadata fromMetadata = it1.next();
						for (Iterator<MMMetadata> it2 = toMetadatas.iterator(); it2.hasNext();) {
							MMMetadata toMetadata = it2.next();
							MMDDependency dependency = new MMDDependency();
							dependency.setOwnerMetadata(fromMetadata);
							dependency.setValueMetadata(toMetadata);
							dependency.setOwnerRole(depc.getFromRole());
							dependency.setValueRole(depc.getToRole());
							result.add(dependency);
						}
					}
				}
			}
		}
		
		return result;
	}
	
	/**
	 * 根据被组合端的元数据Key，查找组合端的元数据Key，两条路走其中一条：<br>
	 * 1、若是useSql，则已经查询过，可以从Map中根据子元数据找父元数据的Key；<br>
	 * 2、若非useSql，从行数据row中根据映射的字段提取值；
	 * @param row 记录行数据
	 * @param comRel 组合关系
	 * @param comMap 组合数据的Map，可能为null
	 * @param childMetadataKey 被组合元数据的Key
	 * @return 组合元数据的Key。若没有找到返回null。
	 */
	private MdKey getParentMetadataKey(Map<String, Object> row, TRecordRelationship comRel,
			Map<MdKey, MdKey> comMap, MdKey childMetadataKey) {
		MdKey parentKey = null; //上级元数据的键值
		if (comRel.useSql()) {
			if (comMap.containsKey(childMetadataKey)) {
				parentKey = comMap.get(childMetadataKey);
			}
		} else {
			String parentId = extractValue(row, comRel.getFromColumns());
			parentKey = MetadataMap.getMetadataKey(comRel.getFromClassifier(), parentId);
		}
		return parentKey;
	}
	
	/**
	 * 对于非查询关系表获取关联数据的（通过外键关联），要从记录行中提取数据准备。
	 * @param row 记录行
	 * @param depList 依赖关系列表(当前记录行的元模型是依赖关系的被依赖端)
	 */
	private void prepareDependencyRelation(Map<String, Object> row, List<TRecordRelationship> depList) {
		if (depList == null || depList.isEmpty()) {
			return ;
		}
		for (int i = 0; i < depList.size(); i++) {
			TRecordRelationship depc = depList.get(i);
			if (depc.useSql()) { continue; }
			MultiMap map = this.depReferences.get(depc);
			if (map == null) {
				map = new MultiValueMap();
				this.depReferences.put(depc, map);
			}
			String fromId = extractValue(row, depc.getFromColumns());
			String toId = extractValue(row, depc.getToColumns());
			map.put(fromId, toId);
		}
	}
	
	/**
	 * 通过查询关系表获取依赖关系的关联数据
	 * @param depc 依赖关系
	 * @return MultiMap，其中KEY请参考{@link #getMetadataKey(String, String)}
	 * @throws SQLException 查询数据库异常
	 */
	private MultiMap queryDependencyRelation(TRecordRelationship depc) throws SQLException {
		if (log.isInfoEnabled()) {
			StringBuffer sb = new StringBuffer();
			sb.append("查询依赖关系：FROM=").append(depc.getFromClassifier());
			sb.append(" TO=").append(depc.getToClassifier());
			sb.append(" FromRole=").append(depc.getFromRole()).append(" ToRole=").append(depc.getToRole());
			sb.append(" FromColumns=").append(depc.getFromColumns()).append(" ToColumns=").append(depc.getToColumns());
			sb.append("  SQL=").append(depc.getRelSqlScript());
			log.info(sb.toString());
		}
		
		MultiMap map = this.depReferences.get(depc);
		if (map == null) {
			map = new MultiValueMap();
			this.depReferences.put(depc, map);
		}
		
		Page page = new Page(0, SIZE);
		ResultSetGetter rs = null;
		while (true) {
			rs = connectionVisitor.query(depc.getRelSqlScript(), page.startIndex, page.pageSize);
			if (log.isInfoEnabled()) {
				log.info("第" + (page.startIndex/page.pageSize + 1) + "次查询返回记录：" + (rs.getRowCount()) + "条");
			}
			for (int i=0, count=rs.getRowCount(); i<count; i++) {
				Map<String, Object> row = rs.getRow(i);
				String fromId = extractValue(row, depc.getFromColumns());
				String toId = extractValue(row, depc.getToColumns());
				MdKey fromKey = MetadataMap.getMetadataKey(depc.getFromClassifier(), fromId);
				MdKey toKey = MetadataMap.getMetadataKey(depc.getToClassifier(), toId);
				if (!this.mdReferences.containsKeyInherit(fromKey, cfg.getInherits())) {
					continue; // 如果From端对应的元数据不存在，抛弃之
				}
				if (!this.mdReferences.containsKeyInherit(toKey, cfg.getInherits())) {
					continue; // 如果To端对应的元数据不存在，抛弃之
				}
				map.put(fromId, toId);
			}
			
			if (rs.getRowCount() < SIZE) { //已经达到最后一页
				break;
			}
			page.next();
		}
		return map;
	}
	
	/**
	 * 通过查询关系表获取组合关系的关联数据
	 * @param comp 组合关系
	 * @return Map，其中KEY为子元数据的Key，VALUE为父元数据的Key。 请参考{@link #getMetadataKey(String, String)}
	 * @throws SQLException 查询数据库异常
	 */
	private Map<MdKey, MdKey> queryCompositionRelation(TRecordRelationship comp) throws SQLException {
		if (log.isInfoEnabled()) {
			StringBuffer sb = new StringBuffer();
			sb.append("查询组合关系：FROM=").append(comp.getFromClassifier());
			sb.append(" TO=").append(comp.getToClassifier());
			sb.append(" FromColumns=").append(comp.getFromColumns()).append(" ToColumns=").append(comp.getToColumns());
			sb.append(" SQL=").append(comp.getRelSqlScript());
			log.info(sb.toString());
		}
		
		Map<MdKey, MdKey> map = new HashMap<MdKey, MdKey>();
		Page page = new Page(0, SIZE);
		ResultSetGetter rs = null;
		while (true) {
			rs = connectionVisitor.query(comp.getRelSqlScript(), page.startIndex, page.pageSize);
			if (log.isInfoEnabled()) {
				log.info("第" + (page.startIndex/page.pageSize + 1) + "次查询返回记录：" + (rs.getRowCount()) + "条");
			}
			for (int i=0, count=rs.getRowCount(); i<count; i++) {
				Map<String, Object> row = rs.getRow(i);
				String fromId = extractValue(row, comp.getFromColumns());
				String toId = extractValue(row, comp.getToColumns());
				MdKey fromKey = MetadataMap.getMetadataKey(comp.getFromClassifier(), fromId);
				if (!this.mdReferences.containsKey(fromKey)) { //如果父元数据不存在，则子元数据就抛弃了
					continue;
				}
				MdKey toKey = MetadataMap.getMetadataKey(comp.getToClassifier(), toId);
				map.put(toKey, fromKey);
			}
			
			if (rs.getRowCount() < SIZE) { //已经达到最后一页
				break;
			}
			page.next();
		}
		return map;
	}
	
	/**
	 * 提取数据
	 * @param rs 查询结果集
	 * @param columnExpr 列表达式，如列名
	 * @return 从行中提取数据
	 */
	protected String extractValue(Map<String,Object> row, String columnExpr) {
		if (StringUtils.isEmpty(columnExpr)) {
			return null;
		}
		Object value = row.get(columnExpr.toUpperCase()); //what if not a column, like OGNL expr?
		if (value == null) {
			value = row.get(columnExpr); //some db donot return uppercase column
		}
		return value == null ? null : String.valueOf(value).trim();
	}
	
	/**
	 * 生成元数据，从数据记录中提取已映射的数据
	 * @param parentMetadata 上级元数据
	 * @param row 数据记录（行）
	 * @param cls 元模型映射
	 * @param fs 属性映射
	 * @return MMMetadata
	 */
	private MMMetadata generateMetadata(MMMetadata parentMetadata, Map<String,Object> row,
			TRecordClassifier cls, List<TRecordFeature> fs) {
		MMMetadata d = new MMMetadata();
		d.setClassifierId(cls.getClassifier());
		d.setParentMetadata(parentMetadata);
		parentMetadata.addChildMetadata(d);
		d.setUid(extractValue(row, cls.getIdRef()));
		d.setCode(extractValue(row, cls.getInstanceCodeRef()));
		d.setName(extractValue(row, cls.getInstanceNameRef()));
		
		Map<String,String> features = new HashMap<String,String>();
		for (int j=0; fs != null && j<fs.size(); j++) {
			TRecordFeature f = fs.get(j);
			features.put(f.getFeatureCode(), extractValue(row, f.getColumnExpr()));
		}
		d.setAttrs(features);
		return d;
	}
	
	/**
	 * 查询上级元模型是否已经拥有指定的元模型，若没有则增加之
	 * @param parentMetamodel 上级元模型
	 * @param classifier 元模型代码
	 * @return 找到的元模型或增加的元模型
	 */
	private MMMetaModel getMetamodel(MMMetaModel parentMetamodel, String classifier) {
		MMMetaModel mm = parentMetamodel.getChildMetaModel(classifier);
		if (mm == null) {
			mm = parentMetamodel.addChildMetaModel(classifier);
			parentMetamodel.setHasChildMetaModel(true);
		}
		return mm;
	}
	
	
	/**
	 * 页码
	 * @author fbchen
	 * @version 1.0  Date: 2010-1-18 上午11:24:19
	 */
	protected static class Page {
		int startIndex;
		int endIndex;
		int pageSize;
		
		// 构建页码
		public Page(int startIndex, int pageSize) {
			this.startIndex = startIndex;
			this.endIndex = startIndex + pageSize;
			this.pageSize = pageSize;
		}
		
		// 下页
		public Page next() {
			this.startIndex = this.endIndex;
			this.endIndex = this.endIndex + this.pageSize;
			return this;
		}
	}
}
