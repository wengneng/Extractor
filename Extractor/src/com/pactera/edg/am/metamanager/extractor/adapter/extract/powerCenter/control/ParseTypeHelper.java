package com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.control;

import java.util.HashMap;
import java.util.Map;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.bo.PcType;
import com.pactera.edg.am.metamanager.extractor.adapter.extract.powerCenter.parse.impl.InstanceParse;

public class ParseTypeHelper {
	private static Map<String, PcType> types = new HashMap<String, PcType>();

	// 初始化需要解析的XML标签
	static {
		// POWERMART（PC集市，悬挂点）
		PcType powerMart = new PcType();
		powerMart.setName("POWERMART");
		types.put(powerMart.getName(), powerMart);

		// REPOSITORY（资源库）
		PcType repository = new PcType();
		repository.setName("REPOSITORY");
		types.put(repository.getName(), repository);

		// FOLDER（文件夹）
		PcType folder = new PcType();
		folder.setName("FOLDER");
		types.put(folder.getName(), folder);

		// SOURCE（源）
		PcType source = new PcType();
		source.setName("SOURCE");
		types.put(source.getName(), source);

		// SOURCEFIELD（源字段）
		PcType sourceField = new PcType();
		sourceField.setName("SOURCEFIELD");
		types.put(sourceField.getName(), sourceField);

		// TARGET（目标）
		PcType target = new PcType();
		target.setName("TARGET");
		types.put(target.getName(), target);

		// TARGETFIELD（源字段）
		PcType targetField = new PcType();
		targetField.setName("TARGETFIELD");
		types.put(targetField.getName(), targetField);

		// TRANSFORMATION（转换）
		PcType transformation = new PcType();
		transformation.setName("TRANSFORMATION");
		types.put(transformation.getName(), transformation);
		
		// TRANSFORMFIELD（转换字段）
		PcType transformField = new PcType();
		transformField.setName("TRANSFORMFIELD");
		types.put(transformField.getName(), transformField);

		// MAPPLET（映射片段）
		PcType mapplet = new PcType();
		mapplet.setName("MAPPLET");
		types.put(mapplet.getName(), mapplet);

		// INSTANCE（实例）
		PcType instance = new PcType();
		instance.setName("INSTANCE");
		instance.setParseDependency(true);
		instance.setParseClass(InstanceParse.class);
		types.put(instance.getName(), instance);

		// MAPPING（映射）
		PcType mapping = new PcType();
		mapping.setName("MAPPING");
		types.put(mapping.getName(), mapping);

	}

	/**
	 * 判断该类型是否在解析范围内
	 * 
	 * @param type
	 * @return
	 */
	public static boolean existType(String typeName) {
		return types.get(typeName) != null;
	}

	/**
	 * 获取类型对象
	 * 
	 * @param typeName
	 * @return
	 */
	public static PcType getPcType(String typeName) {
		return types.get(typeName);
	}

	/**
	 * 根据类型判断是否需要解析依赖关系
	 * 
	 * @param typeName
	 * @return
	 */
	public static boolean isParseDependency(String typeName) {
		if (types.get(typeName) != null) {
			return types.get(typeName).isParseDependency();
		} else {
			return false;
		}
	}

	public static Map<String, PcType> getTypes() {
		return types;
	}

	public static void setTypes(Map<String, PcType> types) {
		ParseTypeHelper.types = types;
	}
}
