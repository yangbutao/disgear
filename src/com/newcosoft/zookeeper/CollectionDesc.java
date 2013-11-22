package com.newcosoft.zookeeper;

import com.newcosoft.cache.Shard;

public class CollectionDesc {
	private String shardId;
	private String collectionName;
	private String roles = null;
	private Integer numShards;
	private String nodeName = null;

	/*
	 * shardRange and shardState are used once-only during sub shard creation
	 * for shard splits Use the values from {@link Slice} instead
	 */
	volatile String shardRange = null;
	volatile String shardState = Shard.ACTIVE;

	volatile boolean isLeader = false;
	volatile String lastPublished = ZkStateReader.ACTIVE;

	public String getLastPublished() {
		return lastPublished;
	}

	public boolean isLeader() {
		return isLeader;
	}

	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	public String getShardId() {
		return shardId;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getRoles() {
		return roles;
	}

	public void setRoles(String roles) {
		this.roles = roles;
	}

	// setting only matters on core creation
	public Integer getNumShards() {
		return numShards;
	}

	public void setNumShards(int numShards) {
		this.numShards = numShards;
	}

	public String getCoreNodeName() {
		return nodeName;
	}

	public void setCoreNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getShardRange() {
		return shardRange;
	}

	public void setShardRange(String shardRange) {
		this.shardRange = shardRange;
	}

	public String getShardState() {
		return shardState;
	}

	public void setShardState(String shardState) {
		this.shardState = shardState;
	}
}
