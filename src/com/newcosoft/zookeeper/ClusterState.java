package com.newcosoft.zookeeper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newcosoft.cache.CacheCollection;
import com.newcosoft.cache.Replica;
import com.newcosoft.cache.Router;
import com.newcosoft.cache.Shard;

public class ClusterState implements JSONWriter.Writable {
	private static Logger log = LoggerFactory.getLogger(ClusterState.class);

	private Integer zkClusterStateVersion;

	private final Map<String, CacheCollection> collectionStates; // Map<collectionName,
																// Map<sliceName,Slice>>
	private final Set<String> liveNodes;

	public ClusterState(Set<String> liveNodes,
			Map<String, CacheCollection> collectionStates) {
		this(null, liveNodes, collectionStates);
	}

	public ClusterState(Integer zkClusterStateVersion, Set<String> liveNodes,
			Map<String, CacheCollection> collectionStates) {
		this.zkClusterStateVersion = zkClusterStateVersion;
		this.liveNodes = new HashSet<String>(liveNodes.size());
		this.liveNodes.addAll(liveNodes);
		this.collectionStates = new HashMap<String, CacheCollection>(
				collectionStates.size());
		this.collectionStates.putAll(collectionStates);
	}

	/**
	 * Get the lead replica for specific collection, or null if one currently
	 * doesn't exist.
	 */
	public Replica getLeader(String collection, String sliceName) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null)
			return null;
		Shard slice = coll.getShard(sliceName);
		if (slice == null)
			return null;
		return slice.getLeader();
	}

	/**
	 * Gets the replica by the core name (assuming the slice is unknown) or null
	 * if replica is not found. If the slice is known, do not use this method.
	 * coreNodeName is the same as replicaName
	 */
	public Replica getReplica(final String collection, final String coreNodeName) {
		return getReplica(collectionStates.get(collection), coreNodeName);
	}

	private Replica getReplica(CacheCollection coll, String replicaName) {
		if (coll == null)
			return null;
		for (Shard slice : coll.getShards()) {
			Replica replica = slice.getReplica(replicaName);
			if (replica != null)
				return replica;
		}
		return null;
	}

	/**
	 * Get the named Slice for collection, or null if not found.
	 */
	public Shard getSlice(String collection, String sliceName) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null)
			return null;
		return coll.getShard(sliceName);
	}

	public Map<String, Shard> getSlicesMap(String collection) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null)
			return null;
		return coll.getShardsMap();
	}

	public Map<String, Shard> getActiveSlicesMap(String collection) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null)
			return null;
		return coll.getActiveShardsMap();
	}

	public Collection<Shard> getSlices(String collection) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null)
			return null;
		return coll.getShards();
	}

	public Collection<Shard> getActiveSlices(String collection) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null)
			return null;
		return coll.getActiveShards();
	}

	/**
	 * Get the named DocCollection object, or throw an exception if it doesn't
	 * exist.
	 */
	public CacheCollection getCollection(String collection) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null) {
			throw new RuntimeException(
					"Could not find collection:" + collection);
		}
		return coll;
	}

	/**
	 * Get collection names.
	 */
	public Set<String> getCollections() {
		return Collections.unmodifiableSet(collectionStates.keySet());
	}

	/**
	 * @return Map&lt;collectionName, Map&lt;sliceName,Slice&gt;&gt;
	 */
	public Map<String, CacheCollection> getCollectionStates() {
		return Collections.unmodifiableMap(collectionStates);
	}

	/**
	 * Get names of the currently live nodes.
	 */
	public Set<String> getLiveNodes() {
		return Collections.unmodifiableSet(liveNodes);
	}

	public String getShardId(String baseUrl, String coreName) {
		// System.out.println("###### getShardId(" + baseUrl + "," + coreName +
		// ") in " + collectionStates);
		for (CacheCollection coll : collectionStates.values()) {
			for (Shard slice : coll.getShards()) {
				for (Replica replica : slice.getReplicas()) {
					// TODO: for really large clusters, we could 'index' on this
					String rbaseUrl = replica
							.getStr(ZkStateReader.BASE_URL_PROP);
					String rcore = replica.getStr(ZkStateReader.CORE_NAME_PROP);
					if (baseUrl.equals(rbaseUrl) && coreName.equals(rcore)) {
						return slice.getName();
					}
				}
			}
		}
		return null;
	}

	/**
	 * Check if node is alive.
	 */
	public boolean liveNodesContain(String name) {
		return liveNodes.contains(name);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("live nodes:" + liveNodes);
		sb.append(" collections:" + collectionStates);
		return sb.toString();
	}

	/**
	 * Create ClusterState by reading the current state from zookeeper.
	 */
	public static ClusterState load(ZooKeeper zkClient, Set<String> liveNodes)
			throws KeeperException, InterruptedException {
		Stat stat = new Stat();
		byte[] state = zkClient.getData(ZkStateReader.CLUSTER_STATE, null,
				stat);
		return load(stat.getVersion(), state, liveNodes);
	}

	/**
	 * Create ClusterState from json string that is typically stored in
	 * zookeeper.
	 * 
	 * Use {@link ClusterState#load(SolrZkClient, Set)} instead, unless you want
	 * to do something more when getting the data - such as get the stat, set
	 * watch, etc.
	 * 
	 * @param version
	 *            zk version of the clusterstate.json file (bytes)
	 * @param bytes
	 *            clusterstate.json as a byte array
	 * @param liveNodes
	 *            list of live nodes
	 * @return the ClusterState
	 */
	public static ClusterState load(Integer version, byte[] bytes,
			Set<String> liveNodes) {
		// System.out.println("######## ClusterState.load:" + (bytes==null ?
		// null : new String(bytes)));
		if (bytes == null || bytes.length == 0) {
			return new ClusterState(version, liveNodes,
					Collections.<String, CacheCollection> emptyMap());
		}
		Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader
				.fromJSON(bytes);
		Map<String, CacheCollection> collections = new LinkedHashMap<String, CacheCollection>(
				stateMap.size());
		for (Entry<String, Object> entry : stateMap.entrySet()) {
			String collectionName = entry.getKey();
			CacheCollection coll = collectionFromObjects(collectionName,
					(Map<String, Object>) entry.getValue());
			collections.put(collectionName, coll);
		}

		// System.out.println("######## ClusterState.load result:" +
		// collections);
		return new ClusterState(version, liveNodes, collections);
	}

	private static CacheCollection collectionFromObjects(String name,
			Map<String, Object> objs) {
		Map<String, Object> props;
		Map<String, Shard> slices;

		Map<String, Object> sliceObjs = (Map<String, Object>) objs
				.get(CacheCollection.SHARDS);
		if (sliceObjs == null) {
			// legacy format from 4.0... there was no separate "shards" level to
			// contain the collection shards.
			slices = makeShards(objs);
			props = Collections.emptyMap();
		} else {
			slices = makeShards(sliceObjs);
			props = new HashMap<String, Object>(objs);
			objs.remove(CacheCollection.SHARDS);
		}

		Router router = Router.DEFAULT;
		return new CacheCollection(name, slices, props, router);
	}

	private static Map<String, Shard> makeShards(
			Map<String, Object> genericSlices) {
		if (genericSlices == null)
			return Collections.emptyMap();
		Map<String, Shard> result = new LinkedHashMap<String, Shard>(
				genericSlices.size());
		for (Map.Entry<String, Object> entry : genericSlices.entrySet()) {
			String name = entry.getKey();
			Object val = entry.getValue();
			if (val instanceof Shard) {
				result.put(name, (Shard) val);
			} else if (val instanceof Map) {
				result.put(name, new Shard(name, null,
						(Map<String, Object>) val));
			}
		}
		return result;
	}

	@Override
	public void write(JSONWriter jsonWriter) {
		jsonWriter.write(collectionStates);
	}

	/**
	 * The version of clusterstate.json in ZooKeeper.
	 * 
	 * @return null if ClusterState was created for publication, not consumption
	 */
	public Integer getZkClusterStateVersion() {
		return zkClusterStateVersion;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((zkClusterStateVersion == null) ? 0 : zkClusterStateVersion
						.hashCode());
		result = prime * result
				+ ((liveNodes == null) ? 0 : liveNodes.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClusterState other = (ClusterState) obj;
		if (zkClusterStateVersion == null) {
			if (other.zkClusterStateVersion != null)
				return false;
		} else if (!zkClusterStateVersion.equals(other.zkClusterStateVersion))
			return false;
		if (liveNodes == null) {
			if (other.liveNodes != null)
				return false;
		} else if (!liveNodes.equals(other.liveNodes))
			return false;
		return true;
	}

}
