package com.newcosoft.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public abstract class Router {

	public static final String DEFAULT_NAME = HashBasedRouter.NAME;
	public static final Router DEFAULT = new HashBasedRouter();

	public Range fromString(String range) {
		int middle = range.indexOf('-');
		String minS = range.substring(0, middle);
		String maxS = range.substring(middle + 1);
		long min = Long.parseLong(minS, 16); 
		long max = Long.parseLong(maxS, 16);
		return new Range((int) min, (int) max);
	}

	public Range fullRange() {
		return new Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
	}

	/**
	 * Returns the range for each partition
	 */
	public List<Range> partitionRange(int partitions, Range range) {
		int min = range.min;
		int max = range.max;

		assert max >= min;
		if (partitions == 0)
			return Collections.EMPTY_LIST;
		long rangeSize = (long) max - (long) min;
		long rangeStep = Math.max(1, rangeSize / partitions);

		List<Range> ranges = new ArrayList<Range>(partitions);

		long start = min;
		long end = start;

		while (end < max) {
			end = start + rangeStep;
			
			if (ranges.size() == partitions - 1) {
				end = max;
			}
			ranges.add(new Range((int) start, (int) end));
			start = end + 1L;
		}

		return ranges;
	}

	public List<Range> partitionRange(int partitions) {
		return partitionRange(partitions, Router.DEFAULT.fullRange());
	}


	public abstract Shard getTargetShard(String key, CacheCollection collection);


	public abstract Collection<Shard> getSearchShardsSingle(String shardKey,
			CacheCollection collection);

	public abstract boolean isTargetShard(String key, String shardId,
			CacheCollection collection);


	public Collection<Shard> getSearchShards(String shardKeys,
			CacheCollection collection) {
		if (shardKeys == null || shardKeys.indexOf(',') < 0) {
			return getSearchShardsSingle(shardKeys, collection);
		}

		List<String> shardKeyList = Util.splitSmart(shardKeys, ",", true);
		HashSet<Shard> allSlices = new HashSet<Shard>();
		for (String shardKey : shardKeyList) {
			allSlices.addAll(getSearchShardsSingle(shardKey, collection));
		}
		return allSlices;
	}

}
