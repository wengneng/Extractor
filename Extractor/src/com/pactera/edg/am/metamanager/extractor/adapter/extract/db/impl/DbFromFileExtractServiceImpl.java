/*
 * Copyright 2009 by pactera.edg.am Corporation. Address:HePingLi East Street No.11
 * 5-5, BeiJing,
 * 
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of pactera.edg.am
 * Corporation ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with pactera.edg.am.
 */

package com.pactera.edg.am.metamanager.extractor.adapter.extract.db.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pactera.edg.am.metamanager.extractor.adapter.extract.db.IDBExtractService;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.core.ModelElement;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Catalog;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Column;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSet;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.NamedColumnSetType;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Procedure;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Schema;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Table;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.Trigger;
import com.pactera.edg.am.metamanager.extractor.bo.cwm.db.View;
import com.pactera.edg.am.metamanager.extractor.util.AdapterExtractorContext;

/**
 * 将存储于.txt中的DB元数据,解析为DB特定的模型对象
 * 
 * @author user
 * @version 1.0 Date: Aug 26, 2009
 * 
 */
public class DbFromFileExtractServiceImpl implements IDBExtractService {

	private Log log = LogFactory.getLog(DbFromFileExtractServiceImpl.class);

	/**
	 * 表与视图开始的标记
	 */
	private final static String TABLE_FLAG = "FROM_TABLE:";

	/**
	 * 字段开始的标记
	 */
	private final static String COLUMN_FLAG = "FROM_COLUMN:";

	/**
	 * 存储过程开始的标记
	 */
	private final static String PROCEDURE_FLAG = "FROM_PROCEDURE:";

	/**
	 * 触发器开发的标记
	 */
	private final static String TRIGGER_FLAG = "FROM_TRIGGER:";

	private final static String SEPARATOR = "\\|";

	private final static String TABLE_MODEL_FLAG = "TABLE";

	/**
	 * 存储DB数据的.txt文件所在文件路径
	 */
	private String filePath;

	/**
	 * schema的缓存,用于将schema.name-->schema缓存,便于在处理table,view,trigger,procedure时方便
	 */
	private Map<String, Schema> schemaCache = new HashMap<String, Schema>(2);

	/**
	 * table与view的缓存:key:schema.name+namedcolumnset.name,value:namedcolumnset,为处理column时便于找到它所属的namedcolumnset
	 */
	private Map<String, NamedColumnSet> columnSetCache = new HashMap<String, NamedColumnSet>();

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pactera.edg.am.metamanager.extractor.adapter.extract.db.IDBExtractService#getCatalog()
	 */
	public Catalog getCatalog() {
		try {
			String filePath = AdapterExtractorContext.getInstance().getDsAbsolutePath();
			return getCatalog(filePath);
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	private Catalog getCatalog(String filePath) throws FileNotFoundException {
		File file = new File(filePath);
		if (!file.exists()) { throw new FileNotFoundException("不存在DB类型的数据源文件:" + filePath); }
		return getCatalog(new FileInputStream(file));
	}

	private Catalog getCatalog(InputStream input) {
		LineNumberReader br = new LineNumberReader(new InputStreamReader(input));
		boolean isTable = false, isColumn = false, isProcedure = false, isTrigger = false;
		String line;
		try {
			while ((line = br.readLine()) != null) {
				if (line.startsWith(TABLE_FLAG)) {
					// 是表或视图
					isTable = true;
					isColumn = false;
					isProcedure = false;
					isTrigger = false;
					// 跳过该行
					continue;
				}
				else if (line.startsWith(COLUMN_FLAG)) {
					// 是表或视图的字段
					isColumn = true;
					isTable = false;
					isProcedure = false;
					isTrigger = false;
					// 跳过该行
					continue;
				}
				else if (line.startsWith(PROCEDURE_FLAG)) {
					// 是存储过程
					isProcedure = true;
					isTable = false;
					isColumn = false;
					isTrigger = false;
					// 跳过该行
					continue;
				}
				else if (line.startsWith(TRIGGER_FLAG)) {
					// 是触发器
					isTrigger = true;
					isTable = false;
					isColumn = false;
					isProcedure = false;
					// 跳过该行
					continue;
				}

				if (isColumn) {
					// 处理COLUMN-->把字段处理放在最前头,因为它的命中率最高
					genColumn(line);
				}
				else if (isTable) {
					// 处理TABLE或VIEW
					genTable(line);
				}
				else if (isProcedure) {
					// 处理PROCEUDRE
					genProcedure(line);
				}
				else if (isTrigger) {
					// 处理TRIGGER
					genTrigger(line);
				}
			}

			return genCatalog();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			// 清理现场
			if (br != null) {
				try {
					br.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}

			schemaCache.clear();
			columnSetCache.clear();
		}
		return null;
	}

	private Catalog genCatalog() {
		Catalog catalog = new Catalog();

		List<Schema> schemas = new ArrayList<Schema>();
		for (Iterator<Schema> iter = schemaCache.values().iterator(); iter.hasNext();) {
			schemas.add(iter.next());
		}
		catalog.setSchemas(schemas);
		return catalog;
	}

	/**
	 * 解析获取所有触发器
	 * 
	 * @param line
	 *            需要解析的字符串
	 */
	private void genTrigger(String line) {
		String[] split = line.split(SEPARATOR);
		if (!schemaCache.containsKey(split[1])) {
			// SCHEMA名还不在缓存中
			Schema schema = new Schema();
			schema.setName(split[1]);
			// 将Schema缓存
			schemaCache.put(split[1], schema);
		}
		Trigger trigger = new Trigger();
		trigger.setName(split[2]);
		trigger.setRemarks(split[3]);

		// 将TRIGGER放置入SCHEMA中
		schemaCache.get(split[1]).addTrigger(trigger);

	}

	/**
	 * 解析获取所有存储过程
	 * 
	 * @param line
	 *            需要解析的字符串
	 */
	private void genProcedure(String line) {
		String[] split = line.split(SEPARATOR);
		if (!schemaCache.containsKey(split[1])) {
			// SCHEMA名还不在缓存中
			Schema schema = new Schema();
			schema.setName(split[1]);
			// 将Schema缓存
			schemaCache.put(split[1], schema);
		}
		Procedure procedure = new Procedure();
		procedure.setName(split[2]);
		// procedure.setRemarks(split[3]);
		procedure.setProcedureType(Integer.valueOf(split[4]).intValue());

		// 将存储过程放置入SCHEMA中
		schemaCache.get(split[1]).addProcedure(procedure);
	}

	/**
	 * 解析获取所有表功视图的字段
	 * 
	 * @param line
	 *            需要解析的字符串
	 */
	private void genColumn(String line) {
		try {
			String[] split = line.split(SEPARATOR);
			if (split.length < 17) {
				log.error("错误的字段:" + Arrays.toString(split));
				return;
			}

			// if(split[2].matches("==\\$0$")){
			// return;
			// }
			Column column = new Column();
			column.setName(split[3]);
			column.setDataType(Integer.valueOf(split[4]).intValue());
			column.setTypeName(split[5]);

			column.setColumnSize(Integer.valueOf(split[6]).intValue());
			column.setBufferLength(Integer.valueOf(split[7]).intValue());
			column.setNumPrecRadix(Integer.valueOf(split[9]).intValue());
			column.setRemarks(split[11]);
			column.addAttr(ModelElement.REMARKS, split[11]);

			column.setColumnDef(split[12]);
			column.setSqlDataType(Integer.valueOf(split[13]).intValue());
			column.setSqlDatetimeSub(Integer.valueOf(split[14]).intValue());
			column.setCharOctetLength(Integer.valueOf(split[15]).intValue());

			column.setOrdinalPosition(Integer.valueOf(split[16]).intValue());
			if ("YES".equals(split[17])) {
				column.setNullable(true);
			}
			else {
				column.setNullable(false);
			}

			// 将字段放置入表或视图中
			columnSetCache.get(split[1] + split[2]).addColumn(column);

		}
		catch (NumberFormatException e) {
			System.err.println("line:" + line);
			e.printStackTrace();
		}

	}

	/**
	 * 解析获取所有表与视图,及SCHEMA(可能不全)
	 * 
	 * @param line
	 *            需要分解的字符串行
	 */
	private void genTable(String line) {
		String[] split = line.split(SEPARATOR);
		if (!schemaCache.containsKey(split[1])) {
			// SCHEMA名还不在缓存中
			Schema schema = new Schema();
			schema.setName(split[1]);
			// 将Schema缓存
			schemaCache.put(split[1], schema);
		}
		// if(split[2].matches("==\\$0$")){
		// return;
		// }

		NamedColumnSet columnSet = null;
		if (TABLE_MODEL_FLAG.equals(split[4])) {
			// TABLE
			columnSet = new Table();
			columnSet.setType(NamedColumnSetType.TABLE);

		}
		else {
			// VIEW
			columnSet = new View();
			columnSet.setType(NamedColumnSetType.VIEW);
		}

		columnSet.setName(split[2]);
		columnSet.addAttr(ModelElement.REMARKS, split[3]);
		columnSet.setTableType(split[4]);

		// 将NamedColumnSet放置入Schema当中
		schemaCache.get(split[1]).addColumnSet(columnSet);
		// 缓存表或视图,key=schema.name+namedcolumnset.name,value=namedcolumnset
		columnSetCache.put(split[1] + split[2], columnSet);
	}

}
