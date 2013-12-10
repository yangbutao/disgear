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

	private final Map<String, CacheCollection> collectionStates; 
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

	
	public Replica getLeader(String collection, String sliceName) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null)
			return null;
		Shard slice = coll.getShard(sliceName);
		if (slice == null)
			return null;
		return slice.getLeader();
	}


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

	
	public CacheCollection getCollection(String collection) {
		CacheCollection coll = collectionStates.get(collection);
		if (coll == null) {
			throw new RuntimeException(
					"Could not find collection:" + collection);
		}
		return coll;
	}

	
	public Set<String> getCollections() {
		return Collections.unmodifiableSet(collectionStates.keySet());
	}

	
	public Map<String, CacheCollection> getCollectionStates() {
		return Collections.unmodifiableMap(collectionStates);
	}

	
	public Set<String> getLiveNodes() {
		return Collections.unmodifiableSet(liveNodes);
	}

	public String getShardId(String baseUrl, String coreName) {
		// System.out.println("###### getShardId(" + baseUrl + "," + coreName +
		// ") in " + collectionStates);
		for (CacheCollection coll : collectionStates.values()) {
			for (Shard slice : coll.getShards()) {
				for (Replica replica : slice.getReplicas()) {
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

	public static ClusterState load(ZooKeeper zkClient, Set<String> liveNodes)
			throws KeeperException, InterruptedException {
		Stat stat = new Stat();
		byte[] state = zkClient.getData(ZkStateReader.CLUSTER_STATE, null,
				stat);
		return load(stat.getVersion(), state, liveNodes);
	}

	
	public static ClusterState load(Integer version, byte[] bytes,
			Set<String> liveNodes) {
		
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

		
		return new ClusterState(version, liveNodes, collections);
	}

	private static CacheCollection collectionFromObjects(String name,
			Map<String, Object> objs) {
		Map<String, Object> props;
		Map<String, Shard> slices;

		Map<String, Object> sliceObjs = (Map<String, Object>) objs
				.get(CacheCollection.SHARDS);
		if (sliceObjs == null) {
			
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

	
	public void write(JSONWriter jsonWriter) {
		jsonWriter.write(collectionStates);
	}

	
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
