package com.newcosoft.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.noggit.JSONUtil;
import org.noggit.JSONWriter;

import com.newcosoft.zookeeper.ZkNodeProps;

public class CacheCollection extends ZkNodeProps {
	public static final String DOC_ROUTER = "router";
	public static final String SHARDS = "shards";

	private final String name;
	private final Map<String, Shard> shards;
	private final Map<String, Shard> activeShards;
	private final Router router;

	public CacheCollection(String name, Map<String, Shard> shards,
			Map<String, Object> props, Router router) {
		super(props == null ? Collections.<String, Object> emptyMap() : props);
		this.name = name;

		this.shards = shards;
		this.activeShards = new HashMap<String, Shard>();

		Iterator<Map.Entry<String, Shard>> iter = shards.entrySet().iterator();

		while (iter.hasNext()) {
			Map.Entry<String, Shard> shard = iter.next();
			if (shard.getValue().getState().equals(Shard.ACTIVE))
				this.activeShards.put(shard.getKey(), shard.getValue());
		}
		this.router = router;

		assert name != null && shards != null;
	}

	/**
	 * Return collection name.
	 */
	public String getName() {
		return name;
	}

	public Shard getShard(String shardName) {
		return shards.get(shardName);
	}

	public Collection<Shard> getShards() {
		return shards.values();
	}

	public Collection<Shard> getActiveShards() {
		return activeShards.values();
	}

	public Map<String, Shard> getShardsMap() {
		return shards;
	}

	public Map<String, Shard> getActiveShardsMap() {
		return activeShards;
	}

	public Router getRouter() {
		return router;
	}

	@Override
	public String toString() {
		return "DocCollection(" + name + ")=" + JSONUtil.toJSON(this);
	}

	@Override
	public void write(JSONWriter jsonWriter) {
		LinkedHashMap<String, Object> all = new LinkedHashMap<String, Object>(
				shards.size() + 1);
		all.putAll(propMap);
		all.put(SHARDS, shards);
		jsonWriter.write(all);
	}

}
