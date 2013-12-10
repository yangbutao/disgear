package com.newcosoft.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class HashBasedRouter extends BaseRouter {
	public static final String NAME = "compositeId";

	private int separator = '!';

	private int bitsSeparator = '/';
	private int bits = 16;
	private int mask1 = 0xffff0000;
	private int mask2 = 0x0000ffff;

	protected void setBits(int firstBits) {
		this.bits = firstBits;
		// java can't shift 32 bits
		mask1 = firstBits == 0 ? 0 : (-1 << (32 - firstBits));
		mask2 = firstBits == 32 ? 0 : (-1 >>> firstBits);
	}

	protected int getBits(String firstPart, int commaIdx) {
		int v = 0;
		for (int idx = commaIdx + 1; idx < firstPart.length(); idx++) {
			char ch = firstPart.charAt(idx);
			if (ch < '0' || ch > '9')
				return -1;
			v = v * 10 + (ch - '0');
		}
		return v > 32 ? -1 : v;
	}

	@Override
	public int shardHash(String key) {
		int idx = key.indexOf(separator);
		if (idx < 0) {
			return Hash.murmurhash3_x86_32(key, 0, key.length(), 0);
		}

		int m1 = mask1;
		int m2 = mask2;

		String part1 = key.substring(0, idx);
		int commaIdx = part1.indexOf(bitsSeparator);
		if (commaIdx > 0) {
			int firstBits = getBits(part1, commaIdx);
			if (firstBits >= 0) {
				m1 = firstBits == 0 ? 0 : (-1 << (32 - firstBits));
				m2 = firstBits == 32 ? 0 : (-1 >>> firstBits);
				part1 = part1.substring(0, commaIdx);
			}
		}

		String part2 = key.substring(idx + 1);

		int hash1 = Hash.murmurhash3_x86_32(part1, 0, part1.length(), 0);
		int hash2 = Hash.murmurhash3_x86_32(part2, 0, part2.length(), 0);
		return (hash1 & m1) | (hash2 & m2);
	}

	@Override
	public Collection<Shard> getSearchShardsSingle(String shardKey,
			CacheCollection collection) {
		if (shardKey == null) {
			return collection.getActiveShards();
		}
		String id = shardKey;

		int idx = shardKey.indexOf(separator);
		if (idx < 0) {
			return Collections
					.singletonList(hashToShard(
							Hash.murmurhash3_x86_32(id, 0, id.length(), 0),
							collection));
		}

		int m1 = mask1;
		int m2 = mask2;

		String part1 = id.substring(0, idx);
		int bitsSepIdx = part1.indexOf(bitsSeparator);
		if (bitsSepIdx > 0) {
			int firstBits = getBits(part1, bitsSepIdx);
			if (firstBits >= 0) {
				m1 = firstBits == 0 ? 0 : (-1 << (32 - firstBits));
				m2 = firstBits == 32 ? 0 : (-1 >>> firstBits);
				part1 = part1.substring(0, bitsSepIdx);
			}
		}


		int hash1 = Hash.murmurhash3_x86_32(part1, 0, part1.length(), 0);
		int upperBits = hash1 & m1;
		int lowerBound = upperBits;
		int upperBound = upperBits | m2;

		if (m1 == 0) {
			lowerBound = Integer.MIN_VALUE;
			upperBound = Integer.MAX_VALUE;
		}

		Range completeRange = new Range(lowerBound, upperBound);

		List<Shard> targetSlices = new ArrayList<Shard>(1);
		for (Shard slice : collection.getActiveShards()) {
			Range range = slice.getRange();
			if (range != null && range.overlaps(completeRange)) {
				targetSlices.add(slice);
			}
		}

		return targetSlices;
	}

	@Override
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

		long targetStart = min;
		long targetEnd = targetStart;


		boolean round = rangeStep >= (1 << bits) * 16;

		while (end < max) {
			targetEnd = targetStart + rangeStep;
			end = targetEnd;

			if (round && ((end & mask2) != mask2)) {
				int increment = 1 << bits; 
				long roundDown = (end | mask2) - increment;
				long roundUp = (end | mask2) + increment;
				if (end - roundDown < roundUp - end && roundDown > start) {
					end = roundDown;
				} else {
					end = roundUp;
				}
			}

			if (ranges.size() == partitions - 1) {
				end = max;
			}
			ranges.add(new Range((int) start, (int) end));
			start = end + 1L;
			targetStart = targetEnd + 1L;
		}

		return ranges;
	}
}
