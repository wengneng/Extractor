/*
 * Copyright 2009 by pactera.edg.am Corporation.
 * Address:HePingLi East Street No.11 5-5, BeiJing, 
 * 
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * pactera.edg.am Corporation ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with pactera.edg.am.
 */

/**
 * 
 */
package com.pactera.edg.am.metamanager.extractor.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;

/**
 * 双向引用MultiMap，其中集成了两个MultiMap。<br>
 * 作用：可以根据Key找到Value，也可以根据Value找到Key。
 *
 * @author fbchen
 * @version 1.0  Date: 2010-1-19 上午10:33:46
 */
public class TwoWayMultiMap implements MultiMap, Serializable {
	private static final long serialVersionUID = 1L;
	
	MultiMap keyMultiMap = new MultiValueMap();
	MultiMap valMultiMap = new MultiValueMap();
	
	/**
	 * 创建双向引用MultiMap
	 */
	public TwoWayMultiMap() {
		super();
	}

	/**
	 * 将key-&lt;value放入keyMultiMap，将value-&lt;key放入valMultiMap
	 * @param key 在keyMultiMap作为Key，在valMultiMap作为Value
	 * @param value 在keyMultiMap作为Value，在valMultiMap作为Key
	 * @return value
	 */
	public Object put(Object key, Object value) {
		keyMultiMap.put(key, value);
		valMultiMap.put(value, key);
		return value;
	}
	
	/**
	 * 是否包含值Value
	 */
	public boolean containsValue(Object value) {
		return valMultiMap.containsKey(value);
	}
	
	/**
	 * 是否包含键Key
	 */
	public boolean containsKey(Object key) {
		return keyMultiMap.containsKey(key);
	}
	
	/**
	 * 根据Value取所有的Key
	 * @param value 值
	 * @return 键集合
	 */
	public Collection<?> getByValue(Object value) {
		return (Collection<?>)valMultiMap.get(value);
	}
	
	/**
	 * 根据Key取对应的所有的Value
	 * @param key 键
	 * @return 值集合
	 */
	public Collection<?> getByKey(Object key) {
		return (Collection<?>)keyMultiMap.get(key);
	}
	
	/**
	 * 根据Value取对应的第一个Key
	 * @param value 值
	 * @return 键
	 */
	public Object getOneByValue(Object value) {
		Collection<?> c = (Collection<?>)valMultiMap.get(value);
		return c.isEmpty() ? null : c.iterator().next();
	}
	
	/**
	 * 根据Key取对应的第一个Value
	 * @param key 键
	 * @return 值
	 */
	public Object getOneByKey(Object key) {
		Collection<?> c = (Collection<?>)keyMultiMap.get(key);
		return c.isEmpty() ? null : c.iterator().next();
	}

	public Object removeByKey(Object key) {
		return keyMultiMap.remove(key);
	}

	public Object removeByKey(Object key, Object value) {
		return keyMultiMap.remove(key, value);
	}
	
	public Object removeByValue(Object value) {
		return valMultiMap.remove(value);
	}

	public Object removeByValue(Object value, Object key) {
		return valMultiMap.remove(value, key);
	}

	/**
	 * 同时清空keyMultiMap和valMultiMap
	 */
	public void clear() {
		keyMultiMap.clear();
		valMultiMap.clear();
	}

	public boolean isEmpty() {
		return keyMultiMap.isEmpty() && valMultiMap.isEmpty();
	}

	
	/**
	 * @return the keyMultiMap
	 */
	public MultiMap getKeyMultiMap() {
		return keyMultiMap;
	}

	/**
	 * @return the valMultiMap
	 */
	public MultiMap getValMultiMap() {
		return valMultiMap;
	}

	
	@SuppressWarnings("unchecked")
	public Set entrySet() {
		return keyMultiMap.entrySet();
	}

	@SuppressWarnings("unchecked")
	public Set keySet() {
		return keyMultiMap.keySet();
	}

	@SuppressWarnings("unchecked")
	public Collection values() {
		return keyMultiMap.values();
	}

	@SuppressWarnings("unchecked")
	public void putAll(Map map) {
		keyMultiMap.putAll(map);
	}

	/**
	 * @see #getByKey(Object)
	 */
	public Object get(Object key) {
		return this.getByKey(key);
	}

	/**
	 * @see #removeByKey(Object,Object)
	 */
	public Object remove(Object key, Object value) {
		return this.removeByKey(key, value);
	}

	/**
	 * @see #removeByKey(Object)
	 */
	public Object remove(Object key) {
		return this.removeByKey(key);
	}

	/**
	 * 返回keyMultiMap的size
	 */
	public int size() {
		return keyMultiMap.size();
	}


}
