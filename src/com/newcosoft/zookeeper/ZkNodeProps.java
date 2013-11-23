package com.newcosoft.zookeeper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.noggit.JSONUtil;
import org.noggit.JSONWriter;

public class ZkNodeProps implements JSONWriter.Writable {

	protected final Map<String, Object> propMap;

	/**
	 * Construct ZKNodeProps from map.
	 */
	public ZkNodeProps(Map<String, Object> propMap) {
		this.propMap = propMap;
	}

	/**
	 * Constructor that populates the from array of Strings in form key1,
	 * value1, key2, value2, ..., keyN, valueN
	 */
	public ZkNodeProps(String... keyVals) {
		this(makeMap((Object[]) keyVals));
	}

	public static ZkNodeProps fromKeyVals(Object... keyVals) {
		return new ZkNodeProps(makeMap(keyVals));
	}

	public static Map<String, Object> makeMap(Object... keyVals) {
		if ((keyVals.length & 0x01) != 0) {
			throw new IllegalArgumentException("arguments should be key,value");
		}
		Map<String, Object> propMap = new HashMap<String, Object>(
				keyVals.length >> 1);
		for (int i = 0; i < keyVals.length; i += 2) {
			propMap.put(keyVals[i].toString(), keyVals[i + 1]);
		}
		return propMap;
	}

	/**
	 * Get property keys.
	 */
	public Set<String> keySet() {
		return propMap.keySet();
	}

	/**
	 * Get all properties as map.
	 */
	public Map<String, Object> getProperties() {
		return propMap;
	}

	/** Returns a shallow writable copy of the properties */
	public Map<String, Object> shallowCopy() {
		return new LinkedHashMap<String, Object>(propMap);
	}

	/**
	 * Create Replica from json string that is typically stored in zookeeper.
	 */
	public static ZkNodeProps load(byte[] bytes) {
		Map<String, Object> props = (Map<String, Object>) ZkStateReader
				.fromJSON(bytes);
		return new ZkNodeProps(props);
	}

	@Override
	public void write(JSONWriter jsonWriter) {
		jsonWriter.write(propMap);
	}

	/**
	 * Get a string property value.
	 */
	public String getStr(String key) {
		Object o = propMap.get(key);
		return o == null ? null : o.toString();
	}

	public Object get(String key) {
		return propMap.get(key);
	}

	@Override
	public String toString() {
		return JSONUtil.toJSON(this);

	}

	/**
	 * Check if property key exists.
	 */
	public boolean containsKey(String key) {
		return propMap.containsKey(key);
	}

}
