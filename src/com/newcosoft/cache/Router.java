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
		long min = Long.parseLong(minS, 16); // use long to prevent the parsing
												// routines from potentially
												// worrying about overflow
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
			// make last range always end exactly on MAX_VALUE
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

	/**
	 * Returns the Slice that the document should reside on, or null if there is
	 * not enough information
	 */
	public abstract Shard getTargetShard(String key, CacheCollection collection);

	/**
	 * This method is consulted to determine what shards should be queried for a
	 * request when an explicit shards parameter was not used. This method only
	 * accepts a single shard key (or null). If you have a comma separated list
	 * of shard keys, call getSearchSlices
	 **/
	public abstract Collection<Shard> getSearchShardsSingle(String shardKey,
			CacheCollection collection);

	public abstract boolean isTargetShard(String key, String shardId,
			CacheCollection collection);

	/**
	 * This method is consulted to determine what slices should be queried for a
	 * request when an explicit shards parameter was not used. This method
	 * accepts a multi-valued shardKeys parameter (normally comma separated from
	 * the shard.keys request parameter) and aggregates the slices returned by
	 * getSearchSlicesSingle for each shardKey.
	 **/
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
