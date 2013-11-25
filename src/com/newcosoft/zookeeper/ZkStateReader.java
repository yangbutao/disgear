package com.newcosoft.zookeeper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newcosoft.cache.Replica;
import com.newcosoft.cache.agent.ShellExec;
import com.newcosoft.util.ByteUtils;

public class ZkStateReader {

	private static Logger log = LoggerFactory.getLogger(ZkStateReader.class);

	public static final String BASE_URL_PROP = "base_url";
	public static final String NODE_NAME_PROP = "node_name";
	public static final String CORE_NODE_NAME_PROP = "core_node_name";
	public static final String ROLES_PROP = "roles";
	public static final String STATE_PROP = "state";
	public static final String CORE_NAME_PROP = "core";
	public static final String COLLECTION_PROP = "collection";
	public static final String SHARD_ID_PROP = "shard";
	public static final String SHARD_RANGE_PROP = "shard_range";
	public static final String SHARD_STATE_PROP = "shard_state";
	public static final String NUM_SHARDS_PROP = "numShards";
	public static final String LEADER_PROP = "leader";

	public static final String COLLECTIONS_ZKNODE = "/collections";
	public static final String LIVE_NODES_ZKNODE = "/live_nodes";
	public static final String ALIASES = "/aliases.json";
	public static final String CLUSTER_STATE = "/clusterstate.json";

	public static final String RECOVERING = "recovering";
	public static final String RECOVERY_FAILED = "recovery_failed";
	public static final String ACTIVE = "active";
	public static final String DOWN = "down";
	public static final String SYNC = "sync";
	public static final String SHARD_LEADERS_ZKNODE = "leaders";
	private ZooKeeper zkClient;
	private volatile ClusterState clusterState;

	private boolean closeClient = false;

	private volatile boolean closed;
	private ZookeeperController zkController;

	public ZkStateReader(ZooKeeper zkClient, ZookeeperController zkController) {
		// TODO Auto-generated constructor stub
		this.zkClient = zkClient;
		this.zkController = zkController;
	}

	public Object getUpdateLock() {
		return this;
	}

	public synchronized void createClusterStateWatchersAndUpdate()
			throws KeeperException, InterruptedException {
		// We need to fetch the current cluster state and the set of live nodes
		synchronized (getUpdateLock()) {
			new ZookeeperClient(zkClient).ensureExists(CLUSTER_STATE, null,
					CreateMode.PERSISTENT);
			new ZookeeperClient(zkClient).ensureExists(ALIASES, null,
					CreateMode.PERSISTENT);

			log.info("Updating cluster state from ZooKeeper... ");

			zkClient.exists(CLUSTER_STATE, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					// session events are not change events,
					// and do not remove the watcher
					if (EventType.None.equals(event.getType())) {
						return;
					}

					try {

						// delayed approach
						// ZkStateReader.this.updateClusterState(false, false);
						synchronized (ZkStateReader.this.getUpdateLock()) {
							// remake watch
							final Watcher thisWatch = this;
							Stat stat = new Stat();
							byte[] data = zkClient.getData(CLUSTER_STATE,
									thisWatch, stat);
							List<String> liveNodes = zkClient.getChildren(
									LIVE_NODES_ZKNODE, this);

							Set<String> liveNodesSet = new HashSet<String>();
							liveNodesSet.addAll(liveNodes);
							Set<String> ln = ZkStateReader.this.clusterState
									.getLiveNodes();
							ClusterState clusterState = ClusterState.load(
									stat.getVersion(), data, liveNodesSet);

							// if leader is changed£¬then change current node slave status to sync from new leader
							String currNodeName = ZkStateReader.this.zkController
									.getNodeName();
							Replica leader = clusterState.getLeader(
									ZkStateReader.this.zkController
											.getCollectionName(),
									ZkStateReader.this.zkController
											.getShardName());
							String leaderNodeName = leader == null ? null
									: leader.getNodeName();
							Replica oldLeader = ZkStateReader.this.clusterState
									.getLeader(ZkStateReader.this.zkController
											.getCollectionName(),
											ZkStateReader.this.zkController
													.getShardName());
							String oldLeaderNodeName = oldLeader == null ? null
									: oldLeader.getNodeName();

							BaseElectionContext electContext = ZkStateReader.this.zkController
									.getElectionContexts().get(currNodeName);
							if (electContext != null
									&& electContext.leaderPath != null) {
								if (zkClient.exists(electContext.leaderPath,
										null) != null) {
									byte[] data2 = zkClient
											.getData(electContext.leaderPath,
													null, stat);

									Map<String, Object> stateMap2 = (Map<String, Object>) ZkStateReader
											.fromJSON(data2);

									// leader is alive

									if (!currNodeName.equals(leaderNodeName)
											&& (leaderNodeName != null & !leaderNodeName
													.equals(oldLeaderNodeName))
											&& liveNodes
													.contains(leaderNodeName)) {
										// current node is slave
										String base_url = (String) stateMap2
												.get("base_url");
										if (base_url != null) {
											String[] args = base_url.split(":");
											ShellExec.runExec(zkController
													.getScriptPath()
													+ File.separator
													+ "redis_slaves.sh "
													+ args[0] + " " + args[1]);
										}
									}
								}

							}

							// update volatile
							ZkStateReader.this.clusterState = clusterState;
							System.out.println(clusterState.toString());

						}
					} catch (KeeperException e) {
						if (e.code() == KeeperException.Code.SESSIONEXPIRED
								|| e.code() == KeeperException.Code.CONNECTIONLOSS) {
							log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
							return;
						}
						log.error("", e);
						throw new RuntimeException(e);
					} catch (InterruptedException e) {
						// Restore the interrupted status
						Thread.currentThread().interrupt();
						log.warn("", e);
						return;
					}
				}

			});
		}

		synchronized (ZkStateReader.this.getUpdateLock()) {
			List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE,
					new Watcher() {

						@Override
						public void process(WatchedEvent event) {
							// session events are not change events,
							// and do not remove the watcher
							if (EventType.None.equals(event.getType())) {
								return;
							}
							try {
								// delayed approach
								// ZkStateReader.this.updateClusterState(false,
								// true);
								synchronized (ZkStateReader.this
										.getUpdateLock()) {
									List<String> liveNodes = zkClient
											.getChildren(LIVE_NODES_ZKNODE,
													this);
									log.info("Updating live nodes... ({})",
											liveNodes.size());
									Set<String> liveNodesSet = new HashSet<String>();
									liveNodesSet.addAll(liveNodes);
									ClusterState clusterState = new ClusterState(
											ZkStateReader.this.clusterState
													.getZkClusterStateVersion(),
											liveNodesSet,
											ZkStateReader.this.clusterState
													.getCollectionStates());
									ZkStateReader.this.clusterState = clusterState;
									System.out.println(clusterState.toString());
								}
							} catch (KeeperException e) {
								if (e.code() == KeeperException.Code.SESSIONEXPIRED
										|| e.code() == KeeperException.Code.CONNECTIONLOSS) {
									log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
									return;
								}
								log.error("", e);
								throw new RuntimeException(e);
							} catch (InterruptedException e) {
								// Restore the interrupted status
								Thread.currentThread().interrupt();
								log.warn("", e);
								return;
							}
						}

					});

			Set<String> liveNodeSet = new HashSet<String>();
			liveNodeSet.addAll(liveNodes);
			ClusterState clusterState = ClusterState
					.load(zkClient, liveNodeSet);
			this.clusterState = clusterState;
		}

	}

	public ClusterState getClusterState() {
		return clusterState;
	}

	public ZooKeeper getZkClient() {
		// TODO Auto-generated method stub
		return zkClient;
	}

	public static String getShardLeadersPath(String collection, String shardId) {
		return COLLECTIONS_ZKNODE + "/" + collection + "/"
				+ SHARD_LEADERS_ZKNODE
				+ (shardId != null ? ("/" + shardId) : "");
	}

	public void updateClusterState(boolean immediate) throws KeeperException,
			InterruptedException {
		updateClusterState(immediate, false);

	}

	// load and publish a new CollectionInfo
	private synchronized void updateClusterState(boolean immediate,
			final boolean onlyLiveNodes) throws KeeperException,
			InterruptedException {
		// build immutable CloudInfo

		if (immediate) {
			ClusterState clusterState;
			synchronized (getUpdateLock()) {
				List<String> liveNodes = zkClient.getChildren(
						LIVE_NODES_ZKNODE, null);
				Set<String> liveNodesSet = new HashSet<String>();
				liveNodesSet.addAll(liveNodes);

				if (!onlyLiveNodes) {
					log.info("Updating cloud state from ZooKeeper... ");

					clusterState = ClusterState.load(zkClient, liveNodesSet);
				} else {
					log.info("Updating live nodes from ZooKeeper... ({})",
							liveNodesSet.size());
					clusterState = new ClusterState(
							ZkStateReader.this.clusterState
									.getZkClusterStateVersion(),
							liveNodesSet, ZkStateReader.this.clusterState
									.getCollectionStates());
				}
				this.clusterState = clusterState;
			}

		} 

	}

	public static byte[] toJSON(Object o) {
		CharArr out = new CharArr();
		new JSONWriter(out, 2).write(o); // indentation by default
		return toUTF8(out);
	}

	public static byte[] toUTF8(CharArr out) {
		byte[] arr = new byte[out.size() << 2]; // is 4x the real worst-case
												// upper-bound?
		int nBytes = ByteUtils.UTF16toUTF8(out, 0, out.size(), arr, 0);
		return Arrays.copyOf(arr, nBytes);
	}

	public static Object fromJSON(byte[] utf8) {
		// convert directly from bytes to chars
		// and parse directly from that instead of going through
		// intermediate strings or readers
		CharArr chars = new CharArr();
		ByteUtils.UTF8toUTF16(utf8, 0, utf8.length, chars);
		JSONParser parser = new JSONParser(chars.getArray(), chars.getStart(),
				chars.length());
		try {
			return ObjectBuilder.getVal(parser);
		} catch (IOException e) {
			throw new RuntimeException(e); // should never happen w/o using real
											// IO
		}
	}

	public void close() {
		this.closed = true;
		if (closeClient) {
			try {
				zkClient.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
