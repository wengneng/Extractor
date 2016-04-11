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

package com.pactera.edg.am.metamanager.extractor.adapter.mapping.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.pactera.edg.am.metamanager.core.util.POIExcelUtil;

/**
 * 数据字典需删除的模板的配置
 * 
 * @author user
 * @version 1.0 Date: Aug 13, 2011
 * 
 */
public class AmTemplateDeleteDataGenerator {

	private static final Set<String> INCLUDING_CLASSIFIERS = new HashSet<String>(5);
	{
		INCLUDING_CLASSIFIERS.add("AmTable");
		INCLUDING_CLASSIFIERS.add("AmColumn");
		INCLUDING_CLASSIFIERS.add("AmColumnCodeItem");
		INCLUDING_CLASSIFIERS.add("AmCommonRealCode");
		INCLUDING_CLASSIFIERS.add("AmCommonRealCodeItem");
	}

	private Workbook wBook;

	private List<DeleteDataBo> deleteDataBos;

	private boolean isUpperCase, isLowerCase;

	public AmTemplateDeleteDataGenerator(Workbook wBook, List<DeleteDataBo> deleteDataBos, boolean isUpperCase,
			boolean isLowerCase)
	{
		this.wBook = wBook;
		this.isUpperCase = isUpperCase;
		this.isLowerCase = isLowerCase;
		this.deleteDataBos = deleteDataBos;
	}

	/**
	 * @return HashMap<元模型,LinkedHashMap<全路径中的元模型, 元数据>>
	 */
	public Map<String, List<Map<String, String>>> generateDeleteData() {
		// 返回的删除数据列表
		Map<String, List<Map<String, String>>> deleteDatas = new HashMap<String, List<Map<String, String>>>(deleteDataBos.size());
		// 先将其反转
		Collections.reverse(deleteDataBos);
		
		for (DeleteDataBo deleteDataBo : deleteDataBos) {
			if (!INCLUDING_CLASSIFIERS.contains(deleteDataBo.getClassifierId())) {
				// 需要处理的元模型不在既定缓存中，认定其不需要处理，跳过
				continue;
			}
			Sheet sheet = wBook.getSheet(deleteDataBo.getSheetName());
			// 数据最大的行数
			int maxRowNum = (deleteDataBo.getDEnd() >= deleteDataBo.getDStart()) ? deleteDataBo.getDEnd() - 1 : sheet
					.getLastRowNum();

			List<Map<String, String>> deleteDataList = new ArrayList<Map<String, String>>();
			for (int rowIndex = deleteDataBo.getDStart() - 1; rowIndex <= maxRowNum; rowIndex++) {
				try {
					Row row = sheet.getRow(rowIndex);

					if (row == null)
						continue;

					boolean rowIsBlank = rowIsBlank(row);
					if (rowIsBlank) // 该行虽然不为NULL,但每一个CELL均为NULL或''，故也不处理之
						continue;

					Map<String, int[]> pathPosition = deleteDataBo.getPathPosition();
					Map<String, String> deleteData = new LinkedHashMap<String, String>(pathPosition.size());
					// 每一层的数据是否正常
					boolean dataIsNormal = true;

					for (Iterator<String> classifierIdIter = pathPosition.keySet().iterator(); classifierIdIter
							.hasNext();) {
						String classifierId = classifierIdIter.next();
						int[] positions = pathPosition.get(classifierId);
						// 会不会有部分为NULL或''的情况呢？该如何处理呢
						String value = getCode(POIExcelUtil.getCellValue(positions, row));
						if (value.equals("")) {
							// 某一个结点为空,应记录一下吧..
							dataIsNormal = false;
							break;
						}
						deleteData.put(classifierId, value);
					}
					if (dataIsNormal) {
						// 数据是正常的,则把数据缓存起来
						deleteDataList.add(deleteData);
					}
				}
				catch (Exception e) {
					// 处理一行数据时，出现异常
					e.printStackTrace();
				}

			}
			deleteDatas.put(deleteDataBo.getClassifierId(), deleteDataList);
		}

		return deleteDatas;
	}

	private String getCode(String cellValue) {
		if (isUpperCase) { return cellValue.toUpperCase(); }
		if (isLowerCase) { return cellValue.toLowerCase(); }
		return cellValue;
	}

	private boolean rowIsBlank(Row row) {
		for (Iterator<Cell> cells = row.iterator(); cells.hasNext();) {
			Cell cell = cells.next();
			if (cell == null)
				continue;
			if (!POIExcelUtil.getCellValue(cell).equals(""))
				return false;

		}
		return true;
	}
}
