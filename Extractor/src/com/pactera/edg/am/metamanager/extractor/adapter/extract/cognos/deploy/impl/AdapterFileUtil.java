package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.deploy.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipFile;

/**
 * @author 游林杰
 * @version 1.0.
 * 
 */
public final class AdapterFileUtil {
	  
	
	
	 /**
	 * 在压缩文件所在的目录中解压，解压后的文件夹的名称与压缩文件相同
	 * @param zipfile
	 */
	public static void unzipAtSameCatalog(String zipFilePath) {
		  List<File> uploadFiles=new ArrayList();
		  File zipfile=new File(zipFilePath);
	    try {
	      String fileName=zipfile.getPath();
	      //防止文件夹与压缩文件重名加上'.catalog'
	      String dirName=fileName+"."+System.currentTimeMillis();
	      File fileDir=new File(dirName);
	      fileDir.mkdir();
	      ZipFile zip = new ZipFile(zipfile);
	      Enumeration enums = zip.getEntries();
	      
	      while (enums.hasMoreElements()) {
	        ZipEntry ze = (ZipEntry) enums.nextElement();
	        File unzipfile = new File(dirName, ze.getName());
	        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(unzipfile));
	        InputStream is = zip.getInputStream(ze);
	        int b;
	        while ((b = is.read()) != -1) {
	          bos.write(b);
	        }
	        bos.close();
	        bos.flush();
	        bos = null;
	        is.close();
	        is = null;
	        uploadFiles.add(unzipfile);
	      }
	      zip.close();
	      zip = null;
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }
	
}
