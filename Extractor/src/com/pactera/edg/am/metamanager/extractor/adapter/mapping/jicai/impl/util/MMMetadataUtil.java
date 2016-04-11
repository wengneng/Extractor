package com.pactera.edg.am.metamanager.extractor.adapter.mapping.jicai.impl.util;

/**    
  * @FileName: MMMetadataUtil.java  
  * @Package:com.vibi.dgp.extractor.adapter.cgb.mapping.util  
  * @Description: TODO 
  * @author: kaishui   
  * @date:2012-11-30 下午2:12:27  
  * @version V1.0    
  */
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsConfig;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateClsTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateMdAttr;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitle;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleCover;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleJoint;
import com.pactera.edg.am.metamanager.app.template.adapter.TemplateTitleJointSet;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/** 
  * @ClassName: MMMetadataUtil  
  * @Description: TODO 
  * @author: kaishui 
  * @date:2012-11-30 下午2:12:27   
  */
public class MMMetadataUtil {
	public static MMMetadataUtil init() {
		return new MMMetadataUtil();
	}
	private String clsId = "";
	// 自底向上，已经处理过的元模型缓存于此
	public Set<TemplateClsConfig> finishedTcConfigs = new HashSet<TemplateClsConfig>();
	/**
	  * 设置采集的元数据库是否可覆盖
	  * @Title: setMetadataCover  
	  * @Description: TODO 
	  * @param @param aMetadata 
	  * @return void 
	  * @throws
	 */
	public void setMetadataCover(List<TemplateClsConfig> tcConfigs) {
		iterTemplateConf(tcConfigs);
//		Map<String, TemplateTitleCover> coverMap = AdapterExtractorContext.getInstance().getIsCover();
//		for(Iterator<String> iter = coverMap.keySet().iterator(); iter
//				.hasNext();){
//			String i = iter.next();
//			System.out.println("ID："+i);
//			TemplateTitleCover templateTitle = coverMap.get(i);
//			System.out.println("-Name："+templateTitle.getMdName()+"-NameCover："+templateTitle.getIsNameCover()+"-Code："+templateTitle.getMdCode()+"-CodeCover："+templateTitle.getIsCodeCover()+"-Att："+templateTitle.getAtt().toString());
//		}
	}

	
	
	/**
	 * 自顶向下根据元模型的树型结构从根开始递归遍历，当遇到元模型为叶子结点时，再自底向上遍历处理每一个元模型
	 * 
	 * @param tcConfigs
	 */
	public void iterTemplateConf(List<TemplateClsConfig> tcConfigs) {
		for (TemplateClsConfig tcConfig : tcConfigs) {

			if (tcConfig.getCompeds().size() == 0) {
				// 自底向上，开始干活
				iterParentTConf(tcConfig);
			}
			else {
				// 组合关系，递归向下
				iterTemplateConf(tcConfig.getCompeds());
			}
		}
	}

	/**
	 * 自底向上遍历处理每一个元模型配置
	 * 
	 * @param tcConfig
	 */
	private void iterParentTConf(TemplateClsConfig tcConfig) {
		if (tcConfig == null) // 根即为没有父结点的配置
			return;
		if (finishedTcConfigs.contains(tcConfig)) // 自底向上，遇到已经处理过的则返回
			return;
		clsId = tcConfig.getClsid();
		List<TemplateClsTitle> tcTitles = tcConfig.getClsTitles();
		for (int index = tcTitles.size() - 1; index >= 0; index--) {
			// 一个元模型的其中一个配置
			// 先处理后面的，再处理前面的，原因在于对于相同数据，后处理的会覆盖先处理的，为了保证数据是第一个配置的，故使用倒序处理
			TemplateClsTitle tcTitle = tcTitles.get(index);
			iterTemplateClsTitle(tcTitle);
		}
		finishedTcConfigs.add(tcConfig);
		iterParentTConf(tcConfig.getParentTcConfig());
	}
	public void iterTemplateClsTitle(TemplateClsTitle tcTitle){
		
		TemplateTitleCover tmplTitle = new TemplateTitleCover();
		//存储该class下的属性值
		Map<String,String> att = new HashMap<String,String>();
		
		/*
		 * 获取mdcode值及覆盖标示
		 */
		TemplateTitleJointSet mset = tcTitle.getMdCode();
		List<TemplateTitleJoint> mj = mset.getTitleJoints();
		for (TemplateTitleJoint templateTitleJoint : mj) {
			List<TemplateTitle> t = templateTitleJoint.getTitles();
			for (TemplateTitle templateTitle : t) {
				tmplTitle.setMdCode(templateTitle.getName());
				tmplTitle.setIsCodeCover(templateTitle.getIsCover());
			}
		}
		/*
		 * 获取 name值及覆盖标识
		 */
		TemplateTitleJointSet nset = tcTitle.getMdName();
		List<TemplateTitleJoint> nj = nset.getTitleJoints();
		for (TemplateTitleJoint templateTitleJoint : nj) {
			List<TemplateTitle> t = templateTitleJoint.getTitles();
			for (TemplateTitle templateTitle : t) {
				tmplTitle.setMdName(templateTitle.getName());
				tmplTitle.setIsNameCover(templateTitle.getIsCover());
			}
		}
		/*
		 * 获取属性数据 及 覆盖标示
		 */
		List<TemplateMdAttr> ma = tcTitle.getMdAttrs();
		for (TemplateMdAttr templateMdAttr : ma) {
			List<TemplateTitleJoint> maj = templateMdAttr.getTitleJnts().getTitleJoints();
			for (TemplateTitleJoint templateTitleJoint : maj) {
				List<TemplateTitle> t = templateTitleJoint.getTitles();
				for (TemplateTitle templateTitle : t) {
					att.put(templateMdAttr.getAttCode(), templateTitle.getIsCover());
				}
			}
		}
		tmplTitle.setAtt(att);
		AdapterExtractorContext.getInstance().getIsCover().put(clsId, tmplTitle);
	}
}
