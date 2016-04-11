package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPathExpressionException;

import net.sf.saxon.Configuration;
import net.sf.saxon.TransformerFactoryImpl;
import net.sf.saxon.jdom.DocumentWrapper;

import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;


/**
 * Xml转换工厂
 * @author wanglei
 * @version 1.0  Date: 2009-9-14 
 */
public abstract class XmlAbstractFactory {
    
	/**
	 * 获得转换所需的xslt文件
	 * @return
	 * @throws Exception
	 */
	abstract String getXsl() throws Exception;
	
	
	abstract boolean parserSource(String xml) throws Exception;
	
	/**
	 * 创建excel文件
	 * @param sourceXml 数据XML文件
	 * @param filename  EXCEL文件名称
	 * @throws Exception
	 */
	public void generateFile(String sourceXml, String filename) throws Exception{
		try{
			if(!parserSource(sourceXml)){
				throw new Exception("语法验证未通过");
			}
			this.transform(sourceXml, getXsl(), filename);
		}catch(TransformerException te){
			throw te;
		}catch(IOException ie){
			throw ie;
		}catch(Exception ex){
			throw ex;
		}
	}
	
	/**
	 * 
	 * @param sourceXml 要进行转换的源文件
	 * @param xslID    转换使用的XSLT
	 * @param filename Excel文件名
	 * @throws TransformerException
	 * @throws IOException
	 */
	/*
    private void transform(String sourceXml, String xslID, String filename)
			throws TransformerException, IOException {
    	
    	ByteArrayInputStream bais = new ByteArrayInputStream(sourceXml.getBytes());
    	//File file = new File(filename);
    	FileOutputStream fos = new FileOutputStream(new File(filename));
    	XSLTUtil.transform(bais, fos, xslID);
    	//File file = new File(filename);

	}
	*/
	
	/**
	 * 
	 * @param sourceID 要进行转换的源文件
	 * @param xslID    转换使用的XSLT
	 * @param filename Excel文件名
	 * @throws TransformerException
	 * @throws XPathExpressionException
	 * @throws JDOMException
	 * @throws IOException
	 */
    private void transform(String sourceID, String xslID, String filename)
			throws TransformerException, XPathExpressionException, JDOMException, IOException {
		// Get a TransformerFactory
		// System.setProperty("javax.xml.transform.TransformerFactory",
		//		"net.sf.saxon.TransformerFactoryImpl");
		TransformerFactory tfactory = TransformerFactory.newInstance();
		Configuration config = ((TransformerFactoryImpl) tfactory).getConfiguration();
		
		InputStream xlstIn = null;
		InputStream xmlIn = null;
		try {
			// Build the JDOM document
			SAXBuilder builder = new SAXBuilder();
			//Document doc = builder.build(new File(sourceID));
			xmlIn = new ByteArrayInputStream(sourceID.getBytes("UTF-8"));
			Document doc = builder.build(xmlIn);
	
			// Give it a Saxon wrapper
			DocumentWrapper docw = new DocumentWrapper(doc, sourceID, config);
	
			// 获取XSLT文件，由于该文件可能在war包里，因此需通过Resource方式获取
			Resource r = new DefaultResourceLoader().getResource(xslID);
			if (r == null || !r.exists()) {
				throw new IOException("无法获取xslt文件：" + xslID);
			}
			xlstIn = r.getInputStream();
			Templates templates = tfactory.newTemplates(new StreamSource(xlstIn));
			Transformer transformer = templates.newTransformer();
	
			// Now do a transformation
			File file = new File(filename);
	
			// transformer.transform(docw, new StreamResult(System.out));
			transformer.transform(docw, new StreamResult(file));
		} finally {
			if (xlstIn != null) {
				xlstIn.close();
			}
			if (xmlIn != null) {
				xmlIn.close();
			}
		}
	}
}
