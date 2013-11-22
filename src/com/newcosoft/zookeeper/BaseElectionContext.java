package com.newcosoft.zookeeper;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newcosoft.cache.Shard;
import com.newcosoft.cache.agent.ShellExec;

public abstract class BaseElectionContext {
	private static Logger log = LoggerFactory.getLogger(BaseElectionContext.class);
	final String electionPath;
	final ZkNodeProps leaderProps;
	final String id;
	final String leaderPath;
	String leaderSeqPath;
	private ZooKeeper zkClient;

	public BaseElectionContext(final String coreNodeName,
			final String electionPath, final String leaderPath,
			final ZkNodeProps leaderProps, final ZooKeeper zkClient) {
		this.id = coreNodeName;
		this.electionPath = electionPath;
		this.leaderPath = leaderPath;
		this.leaderProps = leaderProps;
		this.zkClient = zkClient;
	}

	public void close() {
	}

	public void cancelElection() throws InterruptedException, KeeperException {
		try {
			zkClient.delete(leaderSeqPath, -1);
		} catch (NoNodeException e) {
			// fine
			log.warn("cancelElection did not find election node to remove");
		}

	}

	public String getLeaderSeqPath() {
		return leaderSeqPath;
	}

	public void setLeaderSeqPath(String leaderSeqPath) {
		this.leaderSeqPath = leaderSeqPath;
	}

	abstract void runLeaderProcess(boolean weAreReplacement)
			throws KeeperException, InterruptedException, IOException;

	abstract void rejoinLeaderElection() throws KeeperException,
			InterruptedException, IOException;
}

class ShardLeaderElectionContextBase extends BaseElectionContext {
	private static Logger log = LoggerFactory
			.getLogger(ShardLeaderElectionContextBase.class);
	protected final ZooKeeper zkClient;
	protected String shardId;
	protected String collection;
	protected LeaderElector leaderElector;

	public ShardLeaderElectionContextBase(LeaderElector leaderElector,
			final String shardId, final String collection,
			final String coreNodeName, ZkNodeProps props,
			ZkStateReader zkStateReader) {
		super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
				+ "/leader_elect/" + shardId, ZkStateReader
				.getShardLeadersPath(collection, shardId), props, zkStateReader
				.getZkClient());
		this.leaderElector = leaderElector;
		this.zkClient = zkStateReader.getZkClient();
		this.shardId = shardId;
		this.collection = collection;
	}

	@Override
	void runLeaderProcess(boolean weAreReplacement) throws KeeperException,
			InterruptedException, IOException {

		try {
			new ZookeeperClient(zkClient).makePath(leaderPath,
					ZkStateReader.toJSON(leaderProps), CreateMode.PERSISTENT);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assert shardId != null;
		ZkNodeProps m = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION,
				ZkStateReader.LEADER_PROP, ZkStateReader.SHARD_ID_PROP,
				shardId, ZkStateReader.COLLECTION_PROP, collection,
				ZkStateReader.BASE_URL_PROP,
				leaderProps.getProperties().get(ZkStateReader.BASE_URL_PROP),
				ZkStateReader.CORE_NAME_PROP,
				leaderProps.getProperties().get(ZkStateReader.CORE_NAME_PROP),
				ZkStateReader.STATE_PROP, ZkStateReader.ACTIVE);
		Overseer.getInQueue(zkClient).offer(ZkStateReader.toJSON(m));

	}

	/**
	 * 重新加入选举的过程
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws IOException
	 */
	public void rejoinLeaderElection() throws InterruptedException,
			KeeperException, IOException {
		cancelElection();
		leaderElector.joinElection(this, true);
	}

}

// add core container and stop passing core around...
final class ShardLeaderElectionContext extends ShardLeaderElectionContextBase {
	private static Logger log = LoggerFactory
			.getLogger(ShardLeaderElectionContext.class);

	private ZookeeperController zkController;
	private CollectionContainer cc;

	private volatile boolean isClosed = false;

	public ShardLeaderElectionContext(LeaderElector leaderElector,
			final String shardId, final String collection,
			final String coreNodeName, ZkNodeProps props,
			ZookeeperController zkController, CollectionContainer cc) {
		super(leaderElector, shardId, collection, coreNodeName, props,
				zkController.getZkStateReader());
		this.zkController = zkController;
		this.cc = cc;
	}

	@Override
	public void close() {
		this.isClosed = true;
		// syncStrategy.close();
	}

	/*
	 * weAreReplacement: has someone else been the leader already?
	 */
	@Override
	void runLeaderProcess(boolean weAreReplacement) throws KeeperException,
			InterruptedException, IOException {
		log.info("Running the leader process for shard " + shardId);

		String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);

		// clear the leader in clusterstate
		ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
				ZkStateReader.LEADER_PROP, ZkStateReader.SHARD_ID_PROP,
				shardId, ZkStateReader.COLLECTION_PROP, collection);
		Overseer.getInQueue(zkClient).offer(ZkStateReader.toJSON(m));

		String leaderVoteWait = cc.getZkController().getLeaderVoteWait();

		// 控制redis的master-slave的转换
		// 变更该节点为主节点
		String scriptPath = cc.getZkController().getScriptPath()+File.separator+"redis_master.sh";
		ShellExec.runExec(scriptPath);
		// 其他节点根据clusterstate的shard节点的变更，更新本地的缓存，这样判断当前节点是否需要进行变更
		try {
			super.runLeaderProcess(weAreReplacement);
		} catch (Throwable t) {
			log.error("There was a problem trying to register as the leader", t);
			cancelElection();
			rejoinLeaderElection();
		}

	}

	private void waitForReplicasToComeUp(boolean weAreReplacement,
			String leaderVoteWait) throws InterruptedException {
		int timeout = Integer.parseInt(leaderVoteWait);
		long timeoutAt = System.currentTimeMillis() + timeout;
		final String shardsElectZkPath = electionPath
				+ LeaderElector.ELECTION_NODE;

		Shard slices = zkController.getClusterState().getSlice(collection,
				shardId);
		int cnt = 0;
		while (true && !isClosed) {
			// wait for everyone to be up
			if (slices != null) {
				int found = 0;
				try {
					found = zkClient.getChildren(shardsElectZkPath, null)
							.size();
				} catch (KeeperException e) {
					log.error(
							"Error checking for the number of election participants",
							e);
				}

				// on startup and after connection timeout, wait for all known
				// shards
				if (found >= slices.getReplicasMap().size()) {
					log.info("Enough replicas found to continue.");
					return;
				} else {
					if (cnt % 40 == 0) {
						log.info("Waiting until we see more replicas up for shard "
								+ shardId
								+ ": total="
								+ slices.getReplicasMap().size()
								+ " found="
								+ found
								+ " timeoutin="
								+ (timeoutAt - System.currentTimeMillis()));
					}
				}

				if (System.currentTimeMillis() > timeoutAt) {
					log.info("Was waiting for replicas to come up, but they are taking too long - assuming they won't come back till later");
					return;
				}
			} else {
				log.warn("Shard not found: " + shardId + " for collection "
						+ collection);

				return;

			}

			Thread.sleep(500);
			slices = zkController.getClusterState().getSlice(collection,
					shardId);
			cnt++;
		}
	}

	private void rejoinLeaderElection(String leaderSeqPath)
			throws InterruptedException, KeeperException, IOException {

		log.info("There may be a better leader candidate than us - going back into recovery");

		cancelElection();
		leaderElector.joinElection(this, true);
	}

	private boolean shouldIBeLeader(ZkNodeProps leaderProps,
			boolean weAreReplacement) {
		log.info("Checking if I should try and be the leader.");

		if (isClosed) {
			log.info("Bailing on leader process because we have been closed");
			return false;
		}

		if (!weAreReplacement) {
		
			return true;
		}

		return false;
	}

}
