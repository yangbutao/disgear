package com.newcosoft.cache;

import org.noggit.JSONWriter;

public class Range implements JSONWriter.Writable {
	public int min; // inclusive
	public int max; // inclusive

	public Range(int min, int max) {
		assert min <= max;
		this.min = min;
		this.max = max;
	}

	public boolean includes(int hash) {
		return hash >= min && hash <= max;
	}

	public boolean isSubsetOf(Range superset) {
		return superset.min <= min && superset.max >= max;
	}

	public boolean overlaps(Range other) {
		return includes(other.min) || includes(other.max) || isSubsetOf(other);
	}

	@Override
	public String toString() {
		return Integer.toHexString(min) + '-' + Integer.toHexString(max);
	}

	@Override
	public int hashCode() {
		// difficult numbers to hash... only the highest bits will tend to
		// differ.
		// ranges will only overlap during a split, so we can just hash the
		// lower range.
		return (min >> 28) + (min >> 25) + (min >> 21) + min;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() != getClass())
			return false;
		Range other = (Range) obj;
		return this.min == other.min && this.max == other.max;
	}

	@Override
	public void write(JSONWriter writer) {
		writer.write(toString());
	}
}
