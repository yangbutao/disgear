package com.newcosoft.zookeeper;

import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newcosoft.cache.CacheCollection;
import com.newcosoft.cache.Replica;
import com.newcosoft.cache.Shard;
import com.newcosoft.zookeeper.DistributedQueue.QueueEvent;

public class OverseerCollectionProcessor implements Runnable, ClosableThread {

	public static final String NUM_SLICES = "numShards";

	public static final String REPLICATION_FACTOR = "replicationFactor";

	public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";

	public static final String CREATE_NODE_SET = "createNodeSet";

	public static final String DELETECOLLECTION = "deletecollection";

	public static final String CREATECOLLECTION = "createcollection";

	public static final String RELOADCOLLECTION = "reloadcollection";

	public static final String CREATEALIAS = "createalias";

	public static final String DELETEALIAS = "deletealias";

	public static final String SPLITSHARD = "splitshard";

	public static final String DELETESHARD = "deleteshard";

	// TODO: use from Overseer?
	private static final String QUEUE_OPERATION = "operation";

	private static Logger log = LoggerFactory
			.getLogger(OverseerCollectionProcessor.class);

	private DistributedQueue workQueue;

	private String myId;

	private String adminPath;

	private ZkStateReader zkStateReader;

	private boolean isClosed;

	public OverseerCollectionProcessor(ZkStateReader zkStateReader,
			String myId, String adminPath) {
		this(zkStateReader, myId, adminPath, Overseer
				.getCollectionQueue(zkStateReader.getZkClient()));
	}

	protected OverseerCollectionProcessor(ZkStateReader zkStateReader,
			String myId, String adminPath, DistributedQueue workQueue) {
		this.zkStateReader = zkStateReader;
		this.myId = myId;
		// this.shardHandler = shardHandler;
		this.adminPath = adminPath;
		this.workQueue = workQueue;
	}

	@Override
	public void run() {
		log.info("Process current queue of collection creations");
		while (amILeader() && !isClosed) {
			try {
				// overseer/collection-queue-work(workQueue)
				QueueEvent head = workQueue.peek(true);
				final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
				log.info("Overseer Collection Processor: Get the message id:"
						+ head.getId() + " message:" + message.toString());
				workQueue.remove(head);
				log.info("Overseer Collection Processor: Message id:"
						+ head.getId() + " complete");
			} catch (KeeperException e) {
				if (e.code() == KeeperException.Code.SESSIONEXPIRED
						|| e.code() == KeeperException.Code.CONNECTIONLOSS) {
					log.warn("Overseer cannot talk to ZK");
					return;
				}
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	public void close() {
		isClosed = true;
	}

	protected boolean amILeader() {
		try {
			ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient()
					.getData("/overseer_elect/leader", null, null));
			if (myId.equals(props.getStr("id"))) {
				return true;
			}
		} catch (KeeperException e) {
			log.warn("", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
		return false;
	}

	private String waitForCoreNodeName(CacheCollection collection,
			String msgBaseUrl, String msgCore) {
		int retryCount = 320;
		while (retryCount-- > 0) {
			Map<String, Shard> slicesMap = zkStateReader.getClusterState()
					.getSlicesMap(collection.getName());
			if (slicesMap != null) {

				for (Shard slice : slicesMap.values()) {
					for (Replica replica : slice.getReplicas()) {
						// TODO: for really large clusters, we could 'index' on
						// this

						String baseUrl = replica
								.getStr(ZkStateReader.BASE_URL_PROP);
						String core = replica
								.getStr(ZkStateReader.CORE_NAME_PROP);

						if (baseUrl.equals(msgBaseUrl) && core.equals(msgCore)) {
							return replica.getName();
						}
					}
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		throw new RuntimeException("Could not find coreNodeName");
	}

	private void deleteShard(ClusterState clusterState, ZkNodeProps message) {
		log.info("Delete shard invoked");
		String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

		String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
		Shard slice = clusterState.getSlice(collection, sliceId);

		if (slice == null) {
			if (clusterState.getCollections().contains(collection)) {
				throw new RuntimeException(
						"No shard with the specified name exists: " + slice);
			} else {
				throw new RuntimeException(
						"No collection with the specified name exists: "
								+ collection);
			}
		}
		// For now, only allow for deletions of Inactive slices or custom hashes
		// (range==null).
		// TODO: Add check for range gaps on Slice deletion
		if (!(slice.getRange() == null || slice.getState().equals(
				Shard.INACTIVE))) {
			throw new RuntimeException(
					"The slice: "
							+ slice.getName()
							+ " is currently "
							+ slice.getState()
							+ ". Only INACTIVE (or custom-hashed) slices can be deleted.");
		}

	}

	private Integer msgStrToInt(ZkNodeProps message, String key, Integer def)
			throws Exception {
		String str = message.getStr(key);
		try {
			return str == null ? def : Integer.valueOf(str);
		} catch (Exception ex) {
			log.error("Could not parse " + key, ex);
			throw ex;
		}
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

}
