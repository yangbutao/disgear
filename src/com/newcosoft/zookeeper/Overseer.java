package com.newcosoft.zookeeper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newcosoft.cache.CacheCollection;
import com.newcosoft.cache.Range;
import com.newcosoft.cache.Replica;
import com.newcosoft.cache.Router;
import com.newcosoft.cache.Shard;

public class Overseer {

	public static final String QUEUE_OPERATION = "operation";
	public static final String DELETECORE = "deletecore";
	public static final String REMOVECOLLECTION = "removecollection";
	public static final String REMOVESHARD = "removeshard";

	private static final int STATE_UPDATE_DELAY = 1500; // delay between cloud
														// state updates

	private static Logger log = LoggerFactory.getLogger(Overseer.class);

	private class ClusterStateUpdater implements Runnable, ClosableThread {

		private final ZkStateReader reader;
		private final ZooKeeper zkClient;
		private final String myId;
		// queue where everybody can throw tasks
		private final DistributedQueue stateUpdateQueue;
		// Internal queue where overseer stores events that have not yet been
		// published into cloudstate
		// If Overseer dies while extracting the main queue a new overseer will
		// start from this queue
		private final DistributedQueue workQueue;
		private volatile boolean isClosed;

		/**
		 * 线程服务,循环从queue中获取task，
		 * 
		 * @param reader
		 * @param myId
		 */
		public ClusterStateUpdater(final ZkStateReader reader, final String myId) {
			this.zkClient = reader.getZkClient();
			this.stateUpdateQueue = getInQueue(zkClient);
			this.workQueue = getInternalQueue(zkClient);
			this.myId = myId;
			this.reader = reader;
		}

		@Override
		public void run() {

			if (!this.isClosed && amILeader()) {
				// see if there's something left from the previous Overseer and
				// re
				// process all events that were not persisted into cloud state
				synchronized (reader.getUpdateLock()) { // XXX this only
														// protects
														// against edits inside
														// single
														// node
					try {
						byte[] head = workQueue.peek();

						if (head != null) {
							reader.updateClusterState(true);
							ClusterState clusterState = reader
									.getClusterState();
							log.info("Replaying operations from work queue.");

							while (head != null && amILeader()) {
								final ZkNodeProps message = ZkNodeProps
										.load(head);
								final String operation = message
										.getStr(QUEUE_OPERATION);
								clusterState = processMessage(clusterState,
										message, operation);
								zkClient.setData(ZkStateReader.CLUSTER_STATE,
										ZkStateReader.toJSON(clusterState), -1);

								workQueue.poll();

								head = workQueue.peek();
							}
						}
					} catch (KeeperException e) {
						if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
							log.warn(
									"Solr cannot talk to ZK, exiting Overseer work queue loop",
									e);
							return;
						}
						log.error("Exception in Overseer work queue loop", e);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;

					} catch (Exception e) {
						log.error("Exception in Overseer work queue loop", e);
					}
				}

			}

			log.info("Starting to work on the main queue");
			// /overseer/queue(stateUpdateQueue)===>/overseer/queue-work(workQueue)
			while (!this.isClosed && amILeader()) {
				synchronized (reader.getUpdateLock()) {
					try {
						byte[] head = stateUpdateQueue.peek();
						if (head != null) {
							reader.updateClusterState(true);
							ClusterState clusterState = reader
									.getClusterState();

							while (head != null) {
								final ZkNodeProps message = ZkNodeProps
										.load(head);
								final String operation = message
										.getStr(QUEUE_OPERATION);

								clusterState = processMessage(clusterState,
										message, operation);
								workQueue.offer(head);

								stateUpdateQueue.poll();
								head = stateUpdateQueue.peek();
							}
							zkClient.setData(ZkStateReader.CLUSTER_STATE,
									ZkStateReader.toJSON(clusterState), -1);
						}
						// clean work queue
						while (workQueue.poll() != null)
							;

					} catch (KeeperException e) {
						if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
							log.warn(
									"Solr cannot talk to ZK, exiting Overseer main queue loop",
									e);
							return;
						}
						log.error("Exception in Overseer main queue loop", e);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;

					} catch (Exception e) {
						log.error("Exception in Overseer main queue loop", e);
					}
				}

				try {
					Thread.sleep(STATE_UPDATE_DELAY);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}

		private ClusterState processMessage(ClusterState clusterState,
				final ZkNodeProps message, final String operation) {
			if ("state".equals(operation)) {
				clusterState = updateState(clusterState, message);
			} else if (DELETECORE.equals(operation)) {
				clusterState = removeCore(clusterState, message);
			} else if (REMOVECOLLECTION.equals(operation)) {
				clusterState = removeCollection(clusterState, message);
			} else if (REMOVESHARD.equals(operation)) {
				clusterState = removeShard(clusterState, message);
			} else if (ZkStateReader.LEADER_PROP.equals(operation)) {

				StringBuilder sb = new StringBuilder();
				String baseUrl = message.getStr(ZkStateReader.BASE_URL_PROP);
				String coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);
				sb.append(baseUrl);
				if (baseUrl != null && !baseUrl.endsWith("/"))
					sb.append("/");
				sb.append(coreName == null ? "" : coreName);
				if (!(sb.substring(sb.length() - 1).equals("/")))
					sb.append("/");
				clusterState = setShardLeader(clusterState,
						message.getStr(ZkStateReader.COLLECTION_PROP),
						message.getStr(ZkStateReader.SHARD_ID_PROP),
						sb.length() > 0 ? sb.toString() : null);

			} else if ("createshard".equals(operation)) {
				clusterState = createShard(clusterState, message);
			} else if ("updateshardstate".equals(operation)) {
				clusterState = updateShardState(clusterState, message);
			} else {
				throw new RuntimeException("unknown operation:" + operation
						+ " contents:" + message.getProperties());
			}
			return clusterState;
		}

		private ClusterState updateShardState(ClusterState clusterState,
				ZkNodeProps message) {
			String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
			log.info("Update shard state invoked for collection: " + collection);
			for (String key : message.keySet()) {
				if (ZkStateReader.COLLECTION_PROP.equals(key))
					continue;
				if (QUEUE_OPERATION.equals(key))
					continue;

				Shard slice = clusterState.getSlice(collection, key);
				if (slice == null) {
					throw new RuntimeException(
							"Overseer.updateShardState unknown collection: "
									+ collection + " slice: " + key);
				}
				log.info("Update shard state " + key + " to "
						+ message.getStr(key));
				Map<String, Object> props = slice.shallowCopy();
				props.put(Shard.STATE, message.getStr(key));
				Shard newSlice = new Shard(slice.getName(),
						slice.getReplicasCopy(), props);
				clusterState = updateShard(clusterState, collection, newSlice);
			}

			return clusterState;
		}

		private ClusterState createShard(ClusterState clusterState,
				ZkNodeProps message) {
			String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
			String shardId = message.getStr(ZkStateReader.SHARD_ID_PROP);
			Shard slice = clusterState.getSlice(collection, shardId);
			if (slice == null) {
				Map<String, Replica> replicas = Collections.EMPTY_MAP;
				Map<String, Object> sliceProps = new HashMap<String, Object>();
				String shardRange = message
						.getStr(ZkStateReader.SHARD_RANGE_PROP);
				String shardState = message
						.getStr(ZkStateReader.SHARD_STATE_PROP);
				sliceProps.put(Shard.RANGE, shardRange);
				sliceProps.put(Shard.STATE, shardState);
				slice = new Shard(shardId, replicas, sliceProps);
				clusterState = updateShard(clusterState, collection, slice);
			} else {
				log.error("Unable to create Shard: " + shardId
						+ " because it already exists in collection: "
						+ collection);
			}
			return clusterState;
		}

		private boolean amILeader() {
			try {
				ZkNodeProps props = ZkNodeProps.load(zkClient.getData(
						"/overseer_elect/leader", null, null));
				if (myId.equals(props.getStr("id"))) {
					return true;
				}
			} catch (KeeperException e) {
				log.warn("", e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			log.info("According to ZK I (id=" + myId
					+ ") am no longer a leader.");
			return false;
		}

		/**
		 * Try to assign core to the cluster.
		 */
		private ClusterState updateState(ClusterState state,
				final ZkNodeProps message) {
			final String collection = message
					.getStr(ZkStateReader.COLLECTION_PROP);
			String coreNodeName = message
					.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
			if (coreNodeName == null) {
				coreNodeName = getAssignedCoreNodeName(state, message);
				if (coreNodeName != null) {
					log.info("node=" + coreNodeName + " is already registered");
				} else {
					// if coreNodeName is null, auto assign one
					// coreNodeName = Assign.assignNode(collection, state);
				}
				message.getProperties().put(ZkStateReader.CORE_NODE_NAME_PROP,
						coreNodeName);
			}
			Integer numShards = message.getStr(ZkStateReader.NUM_SHARDS_PROP) != null ? Integer
					.parseInt(message.getStr(ZkStateReader.NUM_SHARDS_PROP))
					: null;
			log.info("Update state numShards={} message={}", numShards, message);
			// collection does not yet exist, create placeholders if num shards
			// is specified
			boolean collectionExists = state.getCollections().contains(
					collection);
			if (!collectionExists && numShards != null) {
				state = createCollection(state, collection, numShards);
			}

			// use the provided non null shardId
			String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
			if (sliceName == null) {
				// get shardId from ClusterState
				sliceName = getAssignedId(state, coreNodeName, message);
				if (sliceName != null) {
					log.info("shard=" + sliceName + " is already registered");
				}
			}
			if (sliceName == null) {
				// request new shardId
				if (collectionExists) {
					// use existing numShards
					numShards = state.getCollectionStates().get(collection)
							.getShards().size();
					log.info("Collection already exists with "
							+ ZkStateReader.NUM_SHARDS_PROP + "=" + numShards);
				}
				// sliceName = Assign.assignShard(collection, state, numShards);
				log.info("Assigning new node to shard shard=" + sliceName);
			}

			Shard slice = state.getSlice(collection, sliceName);

			Map<String, Object> replicaProps = new LinkedHashMap<String, Object>();

			replicaProps.putAll(message.getProperties());
			// System.out.println("########## UPDATE MESSAGE: " +
			// JSONUtil.toJSON(message));
			if (slice != null) {
				String sliceState = slice.getState();

				// throw an exception if the slice is not yet active.

				// if(!sliceState.equals(Slice.ACTIVE)) {
				// throw new SolrException(ErrorCode.BAD_REQUEST,
				// "Can not assign core to a non-active slice [" +
				// slice.getName() + "]");
				// }

				Replica oldReplica = slice.getReplicasMap().get(coreNodeName);
				if (oldReplica != null
						&& oldReplica.containsKey(ZkStateReader.LEADER_PROP)) {
					replicaProps.put(ZkStateReader.LEADER_PROP,
							oldReplica.get(ZkStateReader.LEADER_PROP));
				}
			}

			// we don't put these in the clusterstate
			replicaProps.remove(ZkStateReader.NUM_SHARDS_PROP);
			replicaProps.remove(ZkStateReader.CORE_NODE_NAME_PROP);
			replicaProps.remove(ZkStateReader.SHARD_ID_PROP);
			replicaProps.remove(ZkStateReader.COLLECTION_PROP);
			replicaProps.remove(QUEUE_OPERATION);

			// remove any props with null values
			Set<Entry<String, Object>> entrySet = replicaProps.entrySet();
			List<String> removeKeys = new ArrayList<String>();
			for (Entry<String, Object> entry : entrySet) {
				if (entry.getValue() == null) {
					removeKeys.add(entry.getKey());
				}
			}
			for (String removeKey : removeKeys) {
				replicaProps.remove(removeKey);
			}
			replicaProps.remove(ZkStateReader.CORE_NODE_NAME_PROP);
			// remove shard specific properties
			String shardRange = (String) replicaProps
					.remove(ZkStateReader.SHARD_RANGE_PROP);
			String shardState = (String) replicaProps
					.remove(ZkStateReader.SHARD_STATE_PROP);

			Replica replica = new Replica(coreNodeName, replicaProps);

			// TODO: where do we get slice properties in this message? or should
			// there be a separate create-slice message if we want that?

			Map<String, Object> sliceProps = null;
			Map<String, Replica> replicas;

			if (slice != null) {
				sliceProps = slice.getProperties();
				replicas = slice.getReplicasCopy();
			} else {
				replicas = new HashMap<String, Replica>(1);
				sliceProps = new HashMap<String, Object>();
				sliceProps.put(Shard.RANGE, shardRange);
				sliceProps.put(Shard.STATE, shardState);
			}

			replicas.put(replica.getName(), replica);
			slice = new Shard(sliceName, replicas, sliceProps);

			ClusterState newClusterState = updateShard(state, collection, slice);
			return newClusterState;
		}

		private Map<String, Object> defaultCollectionProps() {
			HashMap<String, Object> props = new HashMap<String, Object>(2);
			props.put(CacheCollection.DOC_ROUTER, Router.DEFAULT_NAME);
			return props;
		}

		private ClusterState createCollection(ClusterState state,
				String collectionName, int numShards) {
			log.info("Create collection {} with numShards {}", collectionName,
					numShards);

			Router router = Router.DEFAULT;
			List<Range> ranges = router.partitionRange(numShards,
					router.fullRange());

			Map<String, CacheCollection> newCollections = new LinkedHashMap<String, CacheCollection>();

			Map<String, Shard> newSlices = new LinkedHashMap<String, Shard>();
			newCollections.putAll(state.getCollectionStates());
			for (int i = 0; i < numShards; i++) {
				final String sliceName = "shard" + (i + 1);

				Map<String, Object> sliceProps = new LinkedHashMap<String, Object>(
						1);
				sliceProps.put(Shard.RANGE, ranges.get(i));

				newSlices
						.put(sliceName, new Shard(sliceName, null, sliceProps));
			}

			// TODO: fill in with collection properties read from the
			// /collections/<collectionName> node
			Map<String, Object> collectionProps = defaultCollectionProps();

			CacheCollection newCollection = new CacheCollection(collectionName,
					newSlices, collectionProps, router);

			newCollections.put(collectionName, newCollection);
			ClusterState newClusterState = new ClusterState(
					state.getLiveNodes(), newCollections);
			return newClusterState;
		}

		/*
		 * Return an already assigned id or null if not assigned
		 */
		private String getAssignedId(final ClusterState state,
				final String nodeName, final ZkNodeProps coreState) {
			Collection<Shard> slices = state.getSlices(coreState
					.getStr(ZkStateReader.COLLECTION_PROP));
			if (slices != null) {
				for (Shard slice : slices) {
					if (slice.getReplicasMap().get(nodeName) != null) {
						return slice.getName();
					}
				}
			}
			return null;
		}

		private String getAssignedCoreNodeName(ClusterState state,
				ZkNodeProps message) {
			Collection<Shard> slices = state.getSlices(message
					.getStr(ZkStateReader.COLLECTION_PROP));
			if (slices != null) {
				for (Shard slice : slices) {
					for (Replica replica : slice.getReplicas()) {
						String baseUrl = replica
								.getStr(ZkStateReader.BASE_URL_PROP);
						String core = replica
								.getStr(ZkStateReader.CORE_NAME_PROP);

						String msgBaseUrl = message
								.getStr(ZkStateReader.BASE_URL_PROP);
						String msgCore = message
								.getStr(ZkStateReader.CORE_NAME_PROP);

						if (baseUrl.equals(msgBaseUrl) && core.equals(msgCore)) {
							return replica.getName();
						}
					}
				}
			}
			return null;
		}

		private ClusterState updateShard(ClusterState state,
				String collectionName, Shard slice) {
			// System.out.println("###!!!### OLD CLUSTERSTATE: " +
			// JSONUtil.toJSON(state.getCollectionStates()));
			// System.out.println("Updating slice:" + slice);

			Map<String, CacheCollection> newCollections = new LinkedHashMap<String, CacheCollection>(
					state.getCollectionStates()); // make a shallow copy
			CacheCollection coll = newCollections.get(collectionName);
			Map<String, Shard> slices;
			Map<String, Object> props;
			Router router = null;

			if (coll == null) {
				// when updateSlice is called on a collection that doesn't
				// exist, it's currently when a core is publishing itself
				// without explicitly creating a collection. In this current
				// case, we assume custom sharding with an "implicit" router.
				slices = new HashMap<String, Shard>(1);
				props = new HashMap<String, Object>(1);
				// props.put(CacheCollection.DOC_ROUTER,
				// ImplicitDocRouter.NAME);
				// router = new ImplicitDocRouter();
			} else {
				props = coll.getProperties();
				router = coll.getRouter();
				slices = new LinkedHashMap<String, Shard>(coll.getShardsMap()); // make
																				// a
																				// shallow
																				// copy
			}
			slices.put(slice.getName(), slice);
			CacheCollection newCollection = new CacheCollection(collectionName,
					slices, props, router);
			newCollections.put(collectionName, newCollection);

			// System.out.println("###!!!### NEW CLUSTERSTATE: " +
			// JSONUtil.toJSON(newCollections));

			return new ClusterState(state.getLiveNodes(), newCollections);
		}

		private ClusterState setShardLeader(ClusterState state,
				String collectionName, String sliceName, String leaderUrl) {

			final Map<String, CacheCollection> newCollections = new LinkedHashMap<String, CacheCollection>(
					state.getCollectionStates());
			CacheCollection coll = newCollections.get(collectionName);
			if (coll == null) {
				log.error("Could not mark shard leader for non existing collection:"
						+ collectionName);
				return state;
			}

			Map<String, Shard> slices = coll.getShardsMap();
			// make a shallow copy and add it to the new collection
			slices = new LinkedHashMap<String, Shard>(slices);

			Shard slice = slices.get(sliceName);
			if (slice == null) {
				slice = coll.getShard(sliceName);
			}

			if (slice == null) {
				log.error("Could not mark leader for non existing/active slice:"
						+ sliceName);
				return state;
			} else {
				// TODO: consider just putting the leader property on the shard,
				// not on individual replicas

				Replica oldLeader = slice.getLeader();

				final Map<String, Replica> newReplicas = new LinkedHashMap<String, Replica>();

				for (Replica replica : slice.getReplicas()) {

					// TODO: this should only be calculated once and cached
					// somewhere?
					String coreURL = ZkCoreNodeProps.getCoreUrl(
							replica.getStr(ZkStateReader.BASE_URL_PROP),
							replica.getStr(ZkStateReader.CORE_NAME_PROP));

					if (replica == oldLeader && !coreURL.equals(leaderUrl)) {
						Map<String, Object> replicaProps = new LinkedHashMap<String, Object>(
								replica.getProperties());
						replicaProps.remove(Shard.LEADER);
						replica = new Replica(replica.getName(), replicaProps);
					} else if (coreURL.equals(leaderUrl)) {
						Map<String, Object> replicaProps = new LinkedHashMap<String, Object>(
								replica.getProperties());
						replicaProps.put(Shard.LEADER, "true"); // TODO: allow
																// booleans
																// instead of
																// strings
						replica = new Replica(replica.getName(), replicaProps);
					}

					newReplicas.put(replica.getName(), replica);
				}

				Map<String, Object> newSliceProps = slice.shallowCopy();
				newSliceProps.put(Shard.REPLICAS, newReplicas);
				Shard newSlice = new Shard(slice.getName(), newReplicas,
						slice.getProperties());
				slices.put(newSlice.getName(), newSlice);
			}

			CacheCollection newCollection = new CacheCollection(coll.getName(),
					slices, coll.getProperties(), coll.getRouter());
			newCollections.put(collectionName, newCollection);
			return new ClusterState(state.getLiveNodes(), newCollections);
		}

		/*
		 * Remove collection from cloudstate
		 */
		private ClusterState removeCollection(final ClusterState clusterState,
				ZkNodeProps message) {

			final String collection = message.getStr("name");

			final Map<String, CacheCollection> newCollections = new LinkedHashMap<String, CacheCollection>(
					clusterState.getCollectionStates()); // shallow copy
			newCollections.remove(collection);

			ClusterState newState = new ClusterState(
					clusterState.getLiveNodes(), newCollections);
			return newState;
		}

		/*
		 * Remove collection slice from cloudstate
		 */
		private ClusterState removeShard(final ClusterState clusterState,
				ZkNodeProps message) {

			final String collection = message
					.getStr(ZkStateReader.COLLECTION_PROP);
			final String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);

			final Map<String, CacheCollection> newCollections = new LinkedHashMap<String, CacheCollection>(
					clusterState.getCollectionStates()); // shallow copy
			CacheCollection coll = newCollections.get(collection);

			Map<String, Shard> newSlices = new LinkedHashMap<String, Shard>(
					coll.getShardsMap());
			newSlices.remove(sliceId);

			CacheCollection newCollection = new CacheCollection(coll.getName(),
					newSlices, new HashMap<String, Object>(), coll.getRouter());
			newCollections.put(newCollection.getName(), newCollection);

			return new ClusterState(clusterState.getLiveNodes(), newCollections);
		}

		/*
		 * Remove core from cloudstate
		 */
		private ClusterState removeCore(final ClusterState clusterState,
				ZkNodeProps message) {

			String cnn = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);

			final String collection = message
					.getStr(ZkStateReader.COLLECTION_PROP);

			final Map<String, CacheCollection> newCollections = new LinkedHashMap<String, CacheCollection>(
					clusterState.getCollectionStates()); // shallow copy
			CacheCollection coll = newCollections.get(collection);
			if (coll == null) {
				// TODO: log/error that we didn't find it?
				// just in case, remove the zk collection node
				try {
					new ZookeeperClient(zkClient).clean("/collections/"
							+ collection);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (KeeperException e) {
					e.printStackTrace();
				}
				return clusterState;
			}

			Map<String, Shard> newSlices = new LinkedHashMap<String, Shard>();
			boolean lastSlice = false;
			for (Shard slice : coll.getShards()) {
				Replica replica = slice.getReplica(cnn);
				if (replica != null) {
					Map<String, Replica> newReplicas = slice.getReplicasCopy();
					newReplicas.remove(cnn);
					// TODO TODO TODO!!! if there are no replicas left for the
					// slice, and the slice has no hash range, remove it
					// if (newReplicas.size() == 0 && slice.getRange() == null)
					// {
					// if there are no replicas left for the slice remove it
					if (newReplicas.size() == 0) {
						slice = null;
						lastSlice = true;
					} else {
						slice = new Shard(slice.getName(), newReplicas,
								new HashMap<String, Object>());
					}
				}

				if (slice != null) {
					newSlices.put(slice.getName(), slice);
				}
			}

			if (lastSlice) {
				// remove all empty pre allocated slices
				for (Shard slice : coll.getShards()) {
					if (slice.getReplicas().size() == 0) {
						newSlices.remove(slice.getName());
					}
				}
			}

			// if there are no slices left in the collection, remove it?
			if (newSlices.size() == 0) {
				newCollections.remove(coll.getName());

				// TODO: it might be better logically to have this in
				// ZkController
				// but for tests (it's easier) it seems better for the moment to
				// leave CoreContainer and/or
				// ZkController out of the Overseer.
				try {
					new ZookeeperClient(zkClient).clean("/collections/"
							+ collection);
				} catch (InterruptedException e) {
					e.printStackTrace();
					Thread.currentThread().interrupt();
				} catch (KeeperException e) {
					e.printStackTrace();
				}

			} else {
				CacheCollection newCollection = new CacheCollection(
						coll.getName(), newSlices,
						new HashMap<String, Object>(), coll.getRouter());
				newCollections.put(newCollection.getName(), newCollection);
			}

			ClusterState newState = new ClusterState(
					clusterState.getLiveNodes(), newCollections);
			return newState;
		}

		@Override
		public void close() {
			this.isClosed = true;
		}

		@Override
		public boolean isClosed() {
			return this.isClosed;
		}

	}

	class OverseerThread extends Thread implements ClosableThread {

		private volatile boolean isClosed;

		public OverseerThread(ThreadGroup tg,
				ClusterStateUpdater clusterStateUpdater) {
			super(tg, clusterStateUpdater);
		}

		public OverseerThread(ThreadGroup ccTg,
				OverseerCollectionProcessor overseerCollectionProcessor,
				String string) {
			super(ccTg, overseerCollectionProcessor, string);
		}

		@Override
		public void close() {
			this.isClosed = true;
		}

		@Override
		public boolean isClosed() {
			return this.isClosed;
		}

	}

	private OverseerThread ccThread;

	private OverseerThread updaterThread;

	private volatile boolean isClosed;

	private ZkStateReader reader;

	private String adminPath;

	public Overseer(final ZkStateReader reader) throws KeeperException,
			InterruptedException {
		this.reader = reader;
	}

	public void start(String id) {
		log.info("Overseer (id=" + id + ") starting");
		createOverseerNode(reader.getZkClient());
		// launch cluster state updater thread
		ThreadGroup tg = new ThreadGroup("Overseer state updater.");
		updaterThread = new OverseerThread(tg, new ClusterStateUpdater(reader,
				id));
		updaterThread.setDaemon(true);

		ThreadGroup ccTg = new ThreadGroup(
				"Overseer collection creation process.");
		ccThread = new OverseerThread(ccTg, new OverseerCollectionProcessor(
				reader, id, adminPath), "Overseer-" + id);
		ccThread.setDaemon(true);

		updaterThread.start();
		ccThread.start();
	}


	public void interuptThread() {
		if (updaterThread != null) {
			updaterThread.interrupt();
		}
		if (ccThread != null) {
			ccThread.interrupt();
		}
	}

	public void close() {
		isClosed = true;
		if (updaterThread != null) {
			try {
				updaterThread.close();
				updaterThread.interrupt();
			} catch (Throwable t) {
				log.error("Error closing updaterThread", t);
			}
		}
		if (ccThread != null) {
			try {
				ccThread.close();
				ccThread.interrupt();
			} catch (Throwable t) {
				log.error("Error closing ccThread", t);
			}
		}

		try {
			reader.close();
		} catch (Throwable t) {
			log.error("Error closing zkStateReader", t);
		}
	}

	/**
	 * Get queue that can be used to send messages to Overseer.
	 */
	public static DistributedQueue getInQueue(final ZooKeeper zkClient) {
		createOverseerNode(zkClient);
		return new DistributedQueue(zkClient, "/overseer/queue", null);
	}

	/* Internal queue, not to be used outside of Overseer */
	static DistributedQueue getInternalQueue(final ZooKeeper zkClient) {
		createOverseerNode(zkClient);
		return new DistributedQueue(zkClient, "/overseer/queue-work", null);
	}

	/* Collection creation queue */
	static DistributedQueue getCollectionQueue(final ZooKeeper zkClient) {
		createOverseerNode(zkClient);
		return new DistributedQueue(zkClient,
				"/overseer/collection-queue-work", null);
	}

	private static void createOverseerNode(final ZooKeeper zkClient) {
		try {
			zkClient.create("/overseer", new byte[0],
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			// ok
			//e.printStackTrace();
		} catch (InterruptedException e) {
			log.error("Could not create Overseer node", e);
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			log.error("Could not create Overseer node", e);
			throw new RuntimeException(e);
		}
	}
}
