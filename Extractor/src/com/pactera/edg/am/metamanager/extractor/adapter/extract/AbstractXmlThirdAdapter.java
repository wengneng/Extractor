package com.pactera.edg.am.metamanager.extractor.adapter.extract;

/**
 * 所有第三方工具xml文件类元数据采集适配的父类
 * @author wanglei
 * @version 1.0  Date: Sept 9, 2009
 */
public abstract class AbstractXmlThirdAdapter extends AbstractXmlAdapter {

	@Override
	protected final String getOutXsd(String templetId){
		return null;
	}
	/**
	 * 第三方工具类转换生成的XML不再进行验证，因此该方法始终返回为空
	 */
	@Override
	protected final String getOutXsd() {
		return null;
	}

	/**
	 * 判断是否第三方工具类
	 */
	@Override
	protected final boolean isThirdParty() {
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
