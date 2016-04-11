package com.pactera.edg.am.metamanager.extractor.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * 持久化到硬盘的工具类
 * 
 * @author user
 * @version 1.0 Date: Jul 7, 2009
 * 
 */
public class PhysisPersitent {

	/**
	 * 将序列化对象持久化到磁盘中
	 * @param obj 序列化对象
	 * @param file 磁盘中的存储路径
	 * @throws IOException
	 */
	public static void write(Object obj, File file) throws IOException {

		ObjectOutput output = new ObjectOutputStream(new FileOutputStream(file));

		// 保存对象

		output.writeObject(obj);

		output.close();

		System.out.println("文件保存在：" + file.getAbsolutePath());

	}

	/**
	 * 从磁盘中将已持久化的数据重新装载入内存中
	 * @param path 磁盘中的存放数据的绝对路径
	 * @return 序列化数据
	 * @throws Exception
	 */
	public static Object load(String path) throws Exception {

		File file = new File(path);

		if (!file.exists()) { return null; }

		ObjectInput ins = new ObjectInputStream(new FileInputStream(file));

		// 读取对象

		return ins.readObject();

	}

}
