package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.deploy.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;

import com.pactera.edg.am.metamanager.core.util.XMLUtil;
import com.pactera.edg.am.metamanager.extractor.increment.impl.GenMetadataServiceImpl;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public class LoadDeployCatalog {
//	private Log log = LogFactory.getLog(LoadDeployCatalog.class);
//	private List<PackageBO> reportPackages = new ArrayList<PackageBO>();
//
//	/**
//	 * @roseuid 4A51B5610119
//	 */
//	public LoadDeployCatalog() {
//
//	}
//
//	/**
//	 * 加载部署文件夹中的XML的内容，模型只记录名称，报表记录存储ID
//	 * 
//	 * @param catalog
//	 * @roseuid 4A51AA1701F4
//	 */
//	public void load(String catalogPath) {
//		// 读取目录
//		File catalogFile = new File(catalogPath);
//
//		// 获取目录下的所有文件
//		File file[] = catalogFile.listFiles();
//
//		for (int i = 0; i < file.length; i++) {
//
//			// 排除content.xml和exportRecord.xml
//			if ("content.xml".equals(file[i].getName())
//					|| "exportRecord.xml".equals(file[i].getName())) {
//				continue;
//			}
//
//			// 对包装箱
//			PackageBO reportPackage = boxPackage(file[i]);
//			// 如果文件中有包，装箱
//			if (reportPackage != null) {
//				reportPackages.add(reportPackage);
//			}
//
//		}
//		log.debug("load success!");
//	}
//
//	/**
//	 * 对当前文件中的包进行装箱
//	 * 
//	 * @param file
//	 * @return
//	 */
//	private PackageBO boxPackage(File file) {
//		// 包vo
//		PackageBO rp = new PackageBO();
//		try {
//			Document xmlDoc = XMLUtil.readDocument(file);
//			Element packageEl = XMLUtil.getChildElement(xmlDoc,
//					"/archive/objects/object[class='package']");
//
//			// 如果文件中没包，返回空
//			if (packageEl == null) {
//				return null;
//			}
//			
//			rp.setCode(packageEl.getChildText("name"));
//			rp.setName(packageEl.getChildText("name"));
//
//			// 该包下的报表文件装箱,因为是包所以节点id为1
//			rp.setReportFiles(this.boxReportFile(xmlDoc, "1"));
//
//			// 该包下的文件夹装箱,因为是包所以父节点id为1
//			rp.setFolders(this.boxFolder(xmlDoc, "1"));
//
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		return rp;
//	}
//
//	/**
//	 * 为父节点下的文件夹装箱，存在递归文件夹
//	 * 
//	 * @param xmlDoc
//	 * @param parentId
//	 * @return
//	 */
//	private List<FolderBO> boxFolder(Document xmlDoc, String parentId) {
//		List<FolderBO> reportFolders = new ArrayList<FolderBO>();
//
//		// 找出所有的文件
//		List<Element> folderEls = XMLUtil.getChildrenElement(xmlDoc,
//				"/archive/objects/object[class='folder' and parentId='"
//						+ parentId + "']");
//
//		// 对所有的文件夹装箱
//		for (int i = 0; i < folderEls.size(); i++) {
//			Element rfEl = (Element) folderEls.get(i);
//			FolderBO rf = new FolderBO();
//			rf.setCode(rfEl.getChildText("name"));
//			rf.setName(rfEl.getChildText("name"));
//
//			// 该文件夹的子文件夹装箱,存在递归文件夹
//			rf.setFolders(this.boxFolder(xmlDoc, rfEl.getChildText("id")));
//
//			// 该文件夹中的报表文件装箱
//			rf.setReportFiles(this.boxReportFile(xmlDoc, rfEl.getChildText("id")));
//
//			reportFolders.add(rf);
//		}
//		return reportFolders;
//	}
//
//	/**
//	 * 对父节点下的报表进行装箱
//	 * 
//	 * @param xmlDoc
//	 * @param parentId
//	 * @return
//	 */
//	private List<ReportFileBO> boxReportFile(Document xmlDoc, String parentId) {
//		List<ReportFileBO> reportFiles = new ArrayList<ReportFileBO>();
//
//		List<Element> reportFileEls = XMLUtil.getChildrenElement(xmlDoc,
//				"/archive/objects/object[class='report' and parentId='"
//						+ parentId + "']");
//
//		for (int i = 0; i < reportFileEls.size(); i++) {
//			Element rfEl = (Element) reportFileEls.get(i);
//			ReportFileBO rf = new ReportFileBO();
//			rf.setCode(rfEl.getChildText("name"));
//			rf.setName(rfEl.getChildText("name"));
//			rf.setStoreID(rfEl.getChildText("storeID"));
//			reportFiles.add(rf);
//		}
//
//		return reportFiles;
//	}
//
//	public List<PackageBO> getReportPackages() {
//		return reportPackages;
//	}
//
//	public Log getLog() {
//		return log;
//	}
//
//	public void setLog(Log log) {
//		this.log = log;
//	}
//
//	public void setReportPackages(List<PackageBO> reportPackages) {
//		this.reportPackages = reportPackages;
//	}

}
