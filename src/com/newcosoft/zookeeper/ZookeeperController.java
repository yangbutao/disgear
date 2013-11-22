package com.newcosoft.zookeeper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newcosoft.cache.CacheCollection;
import com.newcosoft.cache.Replica;
import com.newcosoft.cache.Shard;
import com.newcosoft.cache.agent.ShellExec;

public class ZookeeperController {
	private static Logger log = LoggerFactory
			.getLogger(ZookeeperController.class);
	private ZooKeeper zkClient;
	private DistributedQueue overseerJobQueue;
	private DistributedQueue overseerCollectionQueue;
	private LeaderElector leaderElector;
	private ZkStateReader zkStateReader;
	private String nodeName;
	private String collectionName;
	private String shardName;
	private LeaderElector overseerElector;
	protected volatile Overseer overseer;
	private String baseUrl;
	private CollectionContainer cc;
	private String overseerElectPath;
	private String leaderElectPath;
	private String leaderVoteWait;
	private String scriptPath;
	private static final String LEADER_VOTE_WAIT = "180000"; // 3 minutes

	private BaseElectionContext overseerElectContext;
	private final Map<String, BaseElectionContext> electionContexts = Collections
			.synchronizedMap(new HashMap<String, BaseElectionContext>());

	public ZookeeperController(String serverAddress, int timeout,
			CollectionContainer cc) {
		leaderVoteWait = LEADER_VOTE_WAIT;
		try {
			zkClient = new ZooKeeper(serverAddress, timeout, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.cc = cc;
		setNodeName(cc.getNodeName());
		setCollectionName(cc.getCollectionName());
		setShardName(cc.getShardId());
		setScriptPath(cc.getScriptPath());
		this.overseerJobQueue = Overseer.getInQueue(zkClient);
		this.overseerCollectionQueue = Overseer.getCollectionQueue(zkClient);
		leaderElector = new LeaderElector(zkClient);
		zkStateReader = new ZkStateReader(zkClient, this);
		init();
	}

	public String getScriptPath() {
		return scriptPath;
	}

	public void setScriptPath(String scriptPath) {
		this.scriptPath = scriptPath;
	}

	public String getBaseUrl() {
		return baseUrl;
	}

	public void setBaseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getShardName() {
		return shardName;
	}

	public void setShardName(String shardName) {
		this.shardName = shardName;
	}

	private void init() {

		try {
			boolean createdWatchesAndUpdated = false;
			if (zkClient.exists(ZkStateReader.LIVE_NODES_ZKNODE, null) != null) {
				zkStateReader.createClusterStateWatchersAndUpdate();
				createdWatchesAndUpdated = true;
				publishAndWaitForDownStates();
			}

			// makes nodes zkNode
			new ZookeeperClient(zkClient).ensureExists(
					ZkStateReader.LIVE_NODES_ZKNODE, null,
					CreateMode.PERSISTENT);

			createEphemeralLiveNode();
			new ZookeeperClient(zkClient).ensureExists(
					ZkStateReader.COLLECTIONS_ZKNODE, null,
					CreateMode.PERSISTENT);
			/*
			 * ShardHandler shardHandler; String adminPath; shardHandler =
			 * cc.getShardHandlerFactory().getShardHandler(); adminPath =
			 * cc.getAdminPath();
			 */

			overseerElector = new LeaderElector(zkClient);
			this.overseer = new Overseer(zkStateReader);
			BaseElectionContext context = new OverseerElectionContext2(zkClient,
					overseer, getNodeName(), overseerElector);
			overseerElector.setup(context);
			overseerElectPath = overseerElector.joinElection(context, false);
			overseerElectContext = context;

			if (!createdWatchesAndUpdated) {
				// 创建整个集群cloudstate.json的监听者，更新本地的cloudstate状态
				zkStateReader.createClusterStateWatchersAndUpdate();
			}

		} catch (IOException e) {
			log.error("", e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			// Restore the interrupted status
			Thread.currentThread().interrupt();
			log.error("", e);
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			log.error("", e);
			throw new RuntimeException(e);
		}

	}

	public void publishAndWaitForDownStates() throws KeeperException,
			InterruptedException {
		ClusterState clusterState = zkStateReader.getClusterState();
		Set<String> collections = clusterState.getCollections();
		List<String> updatedNodes = new ArrayList<String>();
		for (String collectionName : collections) {
			CacheCollection collection = clusterState
					.getCollection(collectionName);
			Collection<Shard> slices = collection.getShards();
			for (Shard slice : slices) {
				Collection<Replica> replicas = slice.getReplicas();
				for (Replica replica : replicas) {
					if (replica.getNodeName().equals(getNodeName())
							&& !(replica.getStr(ZkStateReader.STATE_PROP)
									.equals(ZkStateReader.DOWN))) {
						ZkNodeProps m = new ZkNodeProps(
								Overseer.QUEUE_OPERATION, "state",
								ZkStateReader.STATE_PROP, ZkStateReader.DOWN,
								ZkStateReader.BASE_URL_PROP, getBaseUrl(),
								ZkStateReader.CORE_NAME_PROP,
								replica.getStr(ZkStateReader.CORE_NAME_PROP),
								ZkStateReader.ROLES_PROP,
								replica.getStr(ZkStateReader.ROLES_PROP),
								ZkStateReader.NODE_NAME_PROP, getNodeName(),
								ZkStateReader.SHARD_ID_PROP,
								replica.getStr(ZkStateReader.SHARD_ID_PROP),
								ZkStateReader.COLLECTION_PROP, collectionName,
								ZkStateReader.CORE_NODE_NAME_PROP,
								replica.getName());
						updatedNodes.add(replica
								.getStr(ZkStateReader.CORE_NAME_PROP));
						// 放到overseer/queue中，等待Overseer处理
						overseerJobQueue.offer(ZkStateReader.toJSON(m));
					}
				}
			}
		}

		// now wait till the updates are in our state
		long now = System.currentTimeMillis();
		long timeout = now + 1000 * 30;
		boolean foundStates = false;
		while (System.currentTimeMillis() < timeout) {
			clusterState = zkStateReader.getClusterState();
			collections = clusterState.getCollections();
			for (String collectionName : collections) {
				CacheCollection collection = clusterState
						.getCollection(collectionName);
				Collection<Shard> slices = collection.getShards();
				for (Shard slice : slices) {
					Collection<Replica> replicas = slice.getReplicas();
					for (Replica replica : replicas) {
						if (replica.getStr(ZkStateReader.STATE_PROP).equals(
								ZkStateReader.DOWN)) {
							updatedNodes.remove(replica
									.getStr(ZkStateReader.CORE_NAME_PROP));

						}
					}
				}
			}

			if (updatedNodes.size() == 0) {
				foundStates = true;
				Thread.sleep(1000);
				break;
			}
			Thread.sleep(1000);
		}
		if (!foundStates) {
			log.warn("Timed out waiting to see all nodes published as DOWN in our cluster state.");
		}

	}

	public void createCollection(String collection) throws KeeperException,
			InterruptedException {
		ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
				"createcollection", ZkStateReader.NODE_NAME_PROP,
				getNodeName(), ZkStateReader.COLLECTION_PROP, collection);
		overseerJobQueue.offer(ZkStateReader.toJSON(m));
	}

	public String getNodeName() {
		return nodeName;
	}

	/**
	 * Publish core state to overseer.
	 */
	public void publish(final CollectionDesc cd, final String state,
			boolean updateLastState) throws KeeperException,
			InterruptedException {
		log.info("publishing core={} state={}", cd.getCollectionName(), state);
		// System.out.println(Thread.currentThread().getStackTrace()[3]);
		Integer numShards = cd.getNumShards();
		if (numShards == null) { // XXX sys prop hack
			log.info("numShards not found on descriptor - reading it from system property");
			numShards = Integer.getInteger(ZkStateReader.NUM_SHARDS_PROP);
		}

		String coreNodeName = cd.getCoreNodeName();
		// assert cd.getCloudDescriptor().getShardId() != null;
		ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, "state",
				ZkStateReader.STATE_PROP, state, ZkStateReader.BASE_URL_PROP,
				getBaseUrl(), ZkStateReader.CORE_NAME_PROP,
				cd.getCollectionName(), ZkStateReader.ROLES_PROP,
				cd.getRoles(), ZkStateReader.NODE_NAME_PROP, getNodeName(),
				ZkStateReader.SHARD_ID_PROP, cd.getShardId(),
				ZkStateReader.SHARD_RANGE_PROP, cd.getShardRange(),
				ZkStateReader.SHARD_STATE_PROP, cd.getShardState(),
				ZkStateReader.COLLECTION_PROP, cd.getCollectionName(),
				ZkStateReader.NUM_SHARDS_PROP,
				numShards != null ? numShards.toString() : null,
				ZkStateReader.CORE_NODE_NAME_PROP,
				coreNodeName != null ? coreNodeName : null);
		if (updateLastState) {
			cd.lastPublished = state;
		}
		overseerJobQueue.offer(ZkStateReader.toJSON(m));
	}

	public void preRegister(CollectionDesc cd) {
		String collectionNodeName = cd.getCoreNodeName();
		// before becoming available, make sure we are not live and active
		// this also gets us our assigned shard id if it was not specified
		try {
			publish(cd, ZkStateReader.DOWN, false);
		} catch (KeeperException e) {
			log.error("", e);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.error("", e);
			throw new RuntimeException(e);
		}

		// still wait till we see us in local state
		// TODO:增加实现
		doGetShardIdAndNodeNameProcess(cd);

	}

	private void doGetShardIdAndNodeNameProcess(CollectionDesc cd) {
		final String coreNodeName = cd.getCoreNodeName();

		if (coreNodeName != null) {
			waitForShardId(cd);
		} else {
			// if no explicit coreNodeName, we want to match by base url and
			// core name
			waitForCoreNodeName(cd);
			waitForShardId(cd);
		}
	}

	private void waitForCoreNodeName(CollectionDesc descriptor) {
		int retryCount = 320;
		log.info("look for our core node name");
		while (retryCount-- > 0) {
			Map<String, Shard> slicesMap = zkStateReader.getClusterState()
					.getSlicesMap(descriptor.getCollectionName());
			if (slicesMap != null) {

				for (Shard slice : slicesMap.values()) {
					for (Replica replica : slice.getReplicas()) {
						// TODO: for really large clusters, we could 'index' on
						// this

						String baseUrl = replica
								.getStr(ZkStateReader.BASE_URL_PROP);
						String core = replica
								.getStr(ZkStateReader.CORE_NAME_PROP);

						String msgBaseUrl = getBaseUrl();
						String msgCore = descriptor.getCollectionName();

						if (baseUrl.equals(msgBaseUrl) && core.equals(msgCore)) {
							descriptor.setCoreNodeName(replica.getName());
							return;
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
	}

	private void waitForShardId(CollectionDesc cd) {
		log.info("waiting to find shard id in clusterstate for "
				+ cd.getCollectionName());
		int retryCount = 320;
		while (retryCount-- > 0) {
			final String shardId = zkStateReader.getClusterState().getShardId(
					getBaseUrl(), cd.getCollectionName());
			if (shardId != null) {
				cd.setShardId(shardId);
				return;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		throw new RuntimeException("Could not get shard id for core: "
				+ cd.getCollectionName());
	}

	private void createEphemeralLiveNode() throws KeeperException,
			InterruptedException {
		String nodeName = getNodeName();
		String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
		log.info("Register node as live in ZooKeeper:" + nodePath);

		try {
			boolean nodeDeleted = true;
			try {
				// we attempt a delete in the case of a quick server bounce -
				// if there was not a graceful shutdown, the node may exist
				// until expiration timeout - so a node won't be created here
				// because
				// it exists, but eventually the node will be removed. So delete
				// in case it exists and create a new node.
				zkClient.delete(nodePath, -1);
			} catch (KeeperException.NoNodeException e) {
				// fine if there is nothing to delete
				// TODO: annoying that ZK logs a warning on us
				nodeDeleted = false;
			}
			if (nodeDeleted) {
				log.info("Found a previous node that still exists while trying to register a new live node "
						+ nodePath
						+ " - removing existing node to create another.");
			}
			new ZookeeperClient(zkClient).makePath(nodePath, null,
					CreateMode.EPHEMERAL);
		} catch (Exception e) {
			// its okay if the node already exists
			e.printStackTrace();
		}
	}

	public void createCollection(CollectionDesc desc) {
		// TODO Auto-generated method stub
		String collectionName = desc.getCollectionName();

		log.info("Check for collection zkNode:" + collectionName);
		String collectionPath = ZkStateReader.COLLECTIONS_ZKNODE + "/"
				+ collectionName;

		try {
			if (zkClient.exists(collectionPath, null) == null) {
				log.info("Creating collection in ZooKeeper:" + collectionName);
				// SolrParams params = cd.getParams();

				try {
					Map<String, Object> collectionProps = new HashMap<String, Object>();
					/*
					 * // TODO: if collection.configName isn't set, and there
					 * isn't already a conf in zk, just use that? String
					 * defaultConfigName =
					 * System.getProperty(COLLECTION_PARAM_PREFIX
					 * +CONFIGNAME_PROP, collection);
					 * 
					 * // params passed in - currently only done via core admin
					 * (create core commmand). if (params != null) {
					 * Iterator<String> iter =
					 * params.getParameterNamesIterator(); while
					 * (iter.hasNext()) { String paramName = iter.next(); if
					 * (paramName.startsWith(COLLECTION_PARAM_PREFIX)) {
					 * collectionProps
					 * .put(paramName.substring(COLLECTION_PARAM_PREFIX
					 * .length()), params.get(paramName)); } }
					 * 
					 * // if the config name wasn't passed in, use the default
					 * if (!collectionProps.containsKey(CONFIGNAME_PROP)) { //
					 * TODO: getting the configName from the collectionPath
					 * should fail since we already know it doesn't exist?
					 * getConfName(collection, collectionPath, collectionProps);
					 * }
					 * 
					 * } else if(System.getProperty("bootstrap_confdir") !=
					 * null) { // if we are bootstrapping a collection, default
					 * the config for // a new collection to the collection we
					 * are bootstrapping
					 * log.info("Setting config for collection:" + collection +
					 * " to " + defaultConfigName);
					 * 
					 * Properties sysProps = System.getProperties(); for (String
					 * sprop : System.getProperties().stringPropertyNames()) {
					 * if (sprop.startsWith(COLLECTION_PARAM_PREFIX)) {
					 * collectionProps
					 * .put(sprop.substring(COLLECTION_PARAM_PREFIX.length()),
					 * sysProps.getProperty(sprop)); } }
					 * 
					 * // if the config name wasn't passed in, use the default
					 * if (!collectionProps.containsKey(CONFIGNAME_PROP))
					 * collectionProps.put(CONFIGNAME_PROP, defaultConfigName);
					 * 
					 * } else if (Boolean.getBoolean("bootstrap_conf")) { // the
					 * conf name should should be the collection name of this
					 * core collectionProps.put(CONFIGNAME_PROP,
					 * cd.getCollectionName()); } else { getConfName(collection,
					 * collectionPath, collectionProps); }
					 * 
					 * if (collectionProps.get(DocCollection.DOC_ROUTER) ==
					 * null) { Object numShards =
					 * collectionProps.get(ZkStateReader.NUM_SHARDS_PROP); if
					 * (numShards == null) { numShards =
					 * System.getProperty(ZkStateReader.NUM_SHARDS_PROP); } if
					 * (numShards == null) {
					 * collectionProps.put(DocCollection.DOC_ROUTER,
					 * ImplicitDocRouter.NAME); } else {
					 * collectionProps.put(CacheCollection.DOC_ROUTER,
					 * Router.DEFAULT_NAME); } }
					 */
					collectionProps.remove(ZkStateReader.NUM_SHARDS_PROP); // we
																			// don't
																			// put
																			// numShards
																			// in
																			// the
																			// collections
																			// properties

					ZkNodeProps zkProps = new ZkNodeProps(collectionProps);
					new ZookeeperClient(zkClient).makePath(collectionPath,
							ZkStateReader.toJSON(zkProps),
							CreateMode.PERSISTENT);

				} catch (KeeperException e) {
					// its okay if the node already exists
					if (e.code() != KeeperException.Code.NODEEXISTS) {
						throw e;
					}
				}
			} else {
				log.info("Collection zkNode exists");
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void registerInZk(CollectionDesc desc) throws Exception {

		final String baseUrl = getBaseUrl();

		final String collection = desc.getCollectionName();

		final String coreZkNodeName = desc.getCoreNodeName();
		assert coreZkNodeName != null : "we should have a coreNodeName by now";

		String shardId = desc.getShardId();

		Map<String, Object> props = new HashMap<String, Object>();
		// we only put a subset of props into the leader node
		props.put(ZkStateReader.BASE_URL_PROP, baseUrl);
		props.put(ZkStateReader.CORE_NAME_PROP, collection);
		props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());

		if (log.isInfoEnabled()) {
			log.info("Register replica - " + " address:" + baseUrl
					+ " collection:" + collection + " shard:" + shardId);
		}

		ZkNodeProps leaderProps = new ZkNodeProps(props);

		try {
			joinElection(desc, false);
		} catch (InterruptedException e) {
			// Restore the interrupted status
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		publish(desc, ZkStateReader.ACTIVE, true);
		// make sure we have an update cluster state right away
		zkStateReader.updateClusterState(true);

		// 初始化，启动本地的redis的slave模式
		// zkClient.getData(electionContexts.get(nodeName).leaderPath, false,
		// null);
		//
		byte[] initData = zkClient.getData(
				electionContexts.get(nodeName).leaderPath, new Watcher() {

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
							// false);
							synchronized (this) {
								// remake watch
								final Watcher thisWatch = this;
								Stat stat = new Stat();
								byte[] data = zkClient.getData(
										electionContexts.get(nodeName).leaderPath,
										thisWatch, stat);

								Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader
										.fromJSON(data);

								// [core=col1, node_name=node1,
								// base_url=10.1.1.26:6379]
								System.out.print(stateMap);

								// 和以前的leader进行比较,如果leader发生变化，
								String leaderNodeName = (String) stateMap
										.get("node_name");
								String nodeName = getNodeName();
								if (!nodeName.equals(leaderNodeName)) {
									// 当前为slave
									String base_url = (String) stateMap
											.get("base_url");
									String[] args = base_url.split(":");
									ShellExec.runExec(scriptPath
											+ File.separator
											+ "redis_slaves.sh " + args[0]
											+ " " + args[1]);
								}

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

				}, null);

		Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader
				.fromJSON(initData);

		// [core=col1, node_name=node1, base_url=10.1.1.26:6379]
		System.out.print(stateMap);
		String leaderNodeName = (String) stateMap.get("node_name");
		String nodeName = getNodeName();
		if (!nodeName.equals(leaderNodeName)) {
			// 当前为slave
			String base_url = (String) stateMap.get("base_url");
			String[] args = base_url.split(":");
			ShellExec.runExec(scriptPath + File.separator + "redis_slaves.sh "
					+ args[0] + " " + args[1]);
		}

	}

	private void joinElection(CollectionDesc desc, boolean b) throws Exception {
		// TODO Auto-generated method stub
		String shardId = desc.getShardId();
		Map<String, Object> props = new HashMap<String, Object>();
		// we only put a subset of props into the leader node
		props.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
		props.put(ZkStateReader.CORE_NAME_PROP, desc.getCollectionName());
		props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());

		final String coreNodeName = desc.getCoreNodeName();
		ZkNodeProps ourProps = new ZkNodeProps(props);
		String collection = desc.getCollectionName();
		BaseElectionContext context = new ShardLeaderElectionContext(leaderElector,
				shardId, collection, coreNodeName, ourProps, this, cc);
		leaderElector.setup(context);
		electionContexts.put(coreNodeName, context);
		leaderElectPath = leaderElector.joinElection(context, false);
	}

	public String getLeaderElectPath() {
		return leaderElectPath;
	}

	public void setLeaderElectPath(String leaderElectPath) {
		this.leaderElectPath = leaderElectPath;
	}

	public ZkStateReader getZkStateReader() {
		return zkStateReader;
	}

	/**
	 * 更新zookeeper中当前节点的状态
	 */
	public void clearNodeStatus(String nodeName) {
		// overseer节点的选举
		try {
			overseerElectContext.cancelElection();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// shard leader的选举
		BaseElectionContext context = electionContexts.get(nodeName);
		/*
		 * context.cancelElection(); electionContexts.remove(nodeName);
		 */
		try {
			context.cancelElection();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 去掉当前的leaderElectPath
		setLeaderElectPath(null);

		// TODO: live的节点的删除
		// createEphemeralLiveNode();
		// String nodeName = getNodeName();
		String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
		log.info("Register node as live in ZooKeeper:" + nodePath);
		try {
			boolean nodeDeleted = true;
			try {
				// we attempt a delete in the case of a quick server bounce -
				// if there was not a graceful shutdown, the node may exist
				// until expiration timeout - so a node won't be created here
				// because
				// it exists, but eventually the node will be removed. So delete
				// in case it exists and create a new node.
				zkClient.delete(nodePath, -1);
			} catch (KeeperException.NoNodeException e) {
				// fine if there is nothing to delete
				// TODO: annoying that ZK logs a warning on us
				nodeDeleted = false;
			}
			if (nodeDeleted) {
				log.info("Found a previous node that still exists while trying to register a new live node "
						+ nodePath
						+ " - removing existing node to create another.");
			}
			/*
			 * new ZookeeperClient(zkClient).makePath(nodePath, null,
			 * CreateMode.EPHEMERAL);
			 */
		} catch (Exception e) {
			// its okay if the node already exists
			e.printStackTrace();
		}

		// 线程的清除
		this.overseer.interuptThread();

	}

	/**
	 * 更新zookeeper中当前节点的状态
	 */
	public void rejoinLeaderElection(String nodeName) {
		// overseer节点的选举
		try {
			overseerElectContext.rejoinLeaderElection();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// shard leader的选举
		BaseElectionContext context = electionContexts.get(nodeName);
		/*
		 * context.cancelElection(); electionContexts.remove(nodeName);
		 */
		try {
			context.rejoinLeaderElection();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// live的节点
		setLeaderElectPath(context.getLeaderSeqPath());

		// 在lives节点下创建节点
		try {
			createEphemeralLiveNode();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// leaderData的跟踪，当redis重启的时，如果不为leader，则调用slave脚本
		try {
			setNode2SlaveStatus();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setNode2SlaveStatus() throws Exception {
		byte[] initData = zkClient.getData(
				electionContexts.get(nodeName).leaderPath, new Watcher() {

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
							// false);
							synchronized (this) {
								// remake watch
								final Watcher thisWatch = this;
								Stat stat = new Stat();
								byte[] data = zkClient.getData(
										electionContexts.get(nodeName).leaderPath,
										thisWatch, stat);

								Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader
										.fromJSON(data);

								// [core=col1, node_name=node1,
								// base_url=10.1.1.26:6379]
								System.out.print(stateMap);

								// 和以前的leader进行比较,如果leader发生变化，
								String leaderNodeName = (String) stateMap
										.get("node_name");
								String nodeName = getNodeName();
								if (!nodeName.equals(leaderNodeName)) {
									// 当前为slave
									String base_url = (String) stateMap
											.get("base_url");
									String[] args = base_url.split(":");
									ShellExec.runExec(scriptPath
											+ File.separator
											+ "redis_slaves.sh " + args[0]
											+ " " + args[1]);
								}

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

				}, null);

		Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader
				.fromJSON(initData);

		// [core=col1, node_name=node1, base_url=10.1.1.26:6379]
		System.out.print(stateMap);
		String leaderNodeName = (String) stateMap.get("node_name");
		String nodeName = getNodeName();
		if (!nodeName.equals(leaderNodeName)) {
			// 当前为slave
			String base_url = (String) stateMap.get("base_url");
			String[] args = base_url.split(":");
			ShellExec.runExec(scriptPath + File.separator + "redis_slaves.sh "
					+ args[0] + " " + args[1]);
		}
	}

	public String getLeaderVoteWait() {
		return leaderVoteWait;
	}

	public void setLeaderVoteWait(String leaderVoteWait) {
		this.leaderVoteWait = leaderVoteWait;
	}

	/**
	 * @return information about the cluster from ZooKeeper
	 */
	public ClusterState getClusterState() {
		return zkStateReader.getClusterState();
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public Map<String, BaseElectionContext> getElectionContexts() {
		return electionContexts;
	}

}
