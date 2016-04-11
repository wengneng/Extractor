package com.pactera.edg.am.metamanager.extractor.adapter.extract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;
import com.pactera.edg.am.metamanager.extractor.util.XSLTUtil;

/**
 * 所有xml文件类元数据采集适配的父类
 * 
 * @author wanglei
 * @version 1.0 Date: Sept 9, 2009
 */
public abstract class AbstractXmlAdapter {
	// private boolean isThirdParty = false;
	private boolean isInNeedValid = false;

	private boolean isOutNeedValid = false;

	private Log log = LogFactory.getLog(AbstractXmlAdapter.class);

	private String path;

	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * 当前适配器是否为第三方工具适配器
	 * 
	 * @return
	 */
	protected abstract boolean isThirdParty();

	/**
	 * 获得当前适配器匹配的工具名称
	 * 
	 * @return
	 */
	protected abstract String getToolName();

	/**
	 * 获得当前适配器匹配的工具版本号
	 * 
	 * @return
	 */
	protected abstract String getVersion();

	/**
	 * 获得用于转换XML的XSLT文件
	 * 
	 * @return
	 */
	protected abstract String getXslt();

	/**
	 * 获得用于验证输入XML有效性的XSD文件或DTD文件
	 * 
	 * @return
	 */
	protected abstract String getInXsdOrDtd();

	/**
	 * 当前XML输入验证类型，返回“XSD”或“DTD”
	 * 
	 * @return "XSD" or "DTD"
	 */
	protected abstract String getInValidType();

	/**
	 * 获得用于验证输出XML有效性的XSD文件
	 * 
	 * @return
	 */
	protected abstract String getOutXsd(String templetId);

	/**
	 * 获得用于验证输出XML有效性的XSD文件
	 * 
	 * @return
	 */
	protected abstract String getOutXsd();

	/**
	 * 针对Excel模板，获得模板编号
	 * 
	 * @return
	 */
	private String getTempletId(OutputStream out) {
		return null;
	}

	/**
	 * 进行XML转换
	 * 
	 * @return
	 * @throws TransformerException
	 *             转换异常
	 */
	protected final ByteArrayOutputStream transform(File file) throws IOException, 
			TransformerConfigurationException, TransformerException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			XSLTUtil.transform(file, baos, this.getXslt());
		}
		catch (TransformerConfigurationException tce) {
			tce.printStackTrace();
			throw tce;
		}
		catch (TransformerException te) {
			te.printStackTrace();
			throw te;
		}

		return baos;
	}

	/**
	 * 验证输入XML是否合法
	 * 
	 * @param in
	 * @throws Exception
	 */
	protected final void validateInput(File in) throws Exception {
		String validType = null;
		boolean isValid = false;
		if (!isInNeedValid) { return; }
		String xsdOrDtd = getInXsdOrDtd();
		if (xsdOrDtd == null) { throw new Exception("无法验证输入有效性"); }
		validType = this.getInValidType();
		// 执行验证
		if (!isValid) {
			// 校验失败
			String error = "不是该适配器匹配的XML";
			throw new Exception(error);
		}
		return;
	}

	/**
	 * 验证输出XML是否合法
	 * 
	 * @param out
	 * @throws Exception
	 */
	protected final void validateOutput(OutputStream out) throws Exception {
		boolean isValid = false;
		String xsd = null;
		String templetId = getTempletId(out);

		if (!this.isOutNeedValid) { return; }

		// 如果解析XML为Excel模板装载的数据，则可以获得templetId编号
		if (templetId == null) {
			xsd = this.getOutXsd();
		}
		else {
			xsd = this.getOutXsd(templetId);
		}
		// 进行验证

		// 验证失败
		if (!isValid) {
			String error = "输出XML不符合XSD定义，请检查XSLT";
			throw new Exception(error);
		}
		return;
	}

	/**
	 * 
	 * @param file
	 * @return
	 * @throws Exception
	 */
	public final InputStream parse() throws TransformerException, Exception {
		String path = AdapterExtractorContext.getInstance().getDsAbsolutePath();
		log.info(new StringBuilder("开始处理数据源资源文件:").append(path).append(", ...").toString());
		try {
			// 从缓存中获取文件的绝对路径
			File file = new File(path);

			return parseFile(file);
		}
		catch (FileNotFoundException fe) {
			log.error("数据源的资源文件不存在,文件路径:" + path);
			throw fe;
		}finally{
			clear();
		}
	}

	/**
	 * 确保做好垃圾清理工作,尤其是类变量,静态变量
	 */
	protected abstract void clear();

	/**
	 * 转换单个文件
	 * 
	 * @param file
	 * @return
	 * @throws TransformerException
	 * @throws Exception
	 */
	private InputStream parseFile(File file) throws TransformerException, Exception {
		ByteArrayOutputStream result = null;
		// 验证输入XML
		validateInput(file);
		// 执行转换
		result = transform(file);
		// 验证输出XML
		validateOutput(result);
		InputStream is = new ByteArrayInputStream(result.toByteArray());
		return is;
	}

}
