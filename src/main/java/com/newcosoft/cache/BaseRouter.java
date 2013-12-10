package com.newcosoft.cache;

import java.util.Collection;
import java.util.Collections;

public abstract class BaseRouter extends Router {

	@Override
	public Shard getTargetShard(String key, CacheCollection collection) {
		int hash = shardHash(key);
		return hashToShard(hash, collection);
	}

	@Override
	public boolean isTargetShard(String key, String shardId, CacheCollection collection) {
		int hash = shardHash(key);
		Range range = collection.getShard(shardId).getRange();
		return range != null && range.includes(hash);
	}

	public int shardHash(String id) {
		return Hash.murmurhash3_x86_32(id, 0, id.length(), 0);
	}

	protected Shard hashToShard(int hash, CacheCollection collection) {
		for (Shard shard : collection.getActiveShards()) {
			Range range = shard.getRange();
			if (range != null && range.includes(hash))
				return shard;
		}
		throw new RuntimeException(
				"No active shard servicing hash code "
						+ Integer.toHexString(hash) + " in " + collection);
	}

	@Override
	public Collection<Shard> getSearchShardsSingle(String shardKey,CacheCollection collection) {
		if (shardKey == null) {
			return collection.getActiveShards();
		}
		Shard slice = getTargetShard(shardKey, collection);
		return slice == null ? Collections.<Shard> emptyList() : Collections
				.singletonList(slice);
	}
}
