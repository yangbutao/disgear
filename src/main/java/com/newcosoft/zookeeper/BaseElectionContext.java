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
	private static Logger log = LoggerFactory
			.getLogger(BaseElectionContext.class);
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
			e.printStackTrace();
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

	public void rejoinLeaderElection() throws InterruptedException,
			KeeperException, IOException {
		cancelElection();
		leaderElector.joinElection(this, true);
	}

}

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

		// controll redis master-slave transform
		// transform to master
		String scriptPath = cc.getZkController().getScriptPath()
				+ File.separator + "redis_master.sh";
		ShellExec.runExec(scriptPath);
		// other nodes update local cache based on clusterstate
		try {
			super.runLeaderProcess(weAreReplacement);
		} catch (Throwable t) {
			t.printStackTrace();
			cancelElection();
			rejoinLeaderElection();
		}

	}

	
	private void rejoinLeaderElection(String leaderSeqPath)
			throws InterruptedException, KeeperException, IOException {

		log.info("rejoin leader election");

		cancelElection();
		leaderElector.joinElection(this, true);
	}

	private boolean shouldIBeLeader(ZkNodeProps leaderProps,
			boolean weAreReplacement) {
		log.info("Checking if I should try and be the leader.");

		if (isClosed) {
			log.info("on leader process because we have been closed");
			return false;
		}

		if (!weAreReplacement) {

			return true;
		}

		return false;
	}

}
