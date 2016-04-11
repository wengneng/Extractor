package com.pactera.edg.am.metamanager.extractor.adapter.extract.excel.test;

import java.util.ArrayList;

import com.pactera.edg.am.metamanager.app.bo.Attribute;
import com.pactera.edg.am.metamanager.app.bo.Classifier;
import com.pactera.edg.am.metamanager.app.bo.Datatype;

public class TestClassQ {
	
	public Classifier getTestCls() throws Exception{
		Classifier tCls = new Classifier();
		Attribute att = new Attribute();
		Datatype dt = new Datatype();
		
		dt.setId("string");
		att.setDatatype(dt);
		att.setLength(12);
		att.setId("cnName");
		ArrayList<Attribute> list = new ArrayList<Attribute>();
		list.add(att);
		tCls.setAttributes(list);
		tCls.setId("Table");
		return tCls;
	}
	
	public Classifier getTestClsView() throws Exception{
		Classifier tCls = new Classifier();
		Attribute att = new Attribute();
		Datatype dt = new Datatype();
		
		dt.setId("string");
		att.setDatatype(dt);
		att.setLength(12);
		att.setId("cnName");
		ArrayList<Attribute> list = new ArrayList<Attribute>();
		list.add(att);
		tCls.setAttributes(list);
		tCls.setId("View");
		return tCls;
	}
	public Classifier getTestClsCol() throws Exception{
		Classifier tCls = new Classifier();
		Attribute att = new Attribute();
		Datatype dt = new Datatype();
		
		dt.setId("string");
		att.setDatatype(dt);
		att.setLength(5);
		att.setId("propet");
		ArrayList<Attribute> list = new ArrayList<Attribute>();
		list.add(att);
		tCls.setAttributes(list);
		tCls.setId("Column");
		return tCls;
	}
	
	public Classifier getClassifier(String classId) throws Exception{
		return null;
	}
}
