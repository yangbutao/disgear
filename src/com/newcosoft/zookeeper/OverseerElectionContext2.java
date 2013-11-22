package com.newcosoft.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class OverseerElectionContext2 extends BaseElectionContext {

	private final ZooKeeper zkClient;
	private Overseer overseer;
	private LeaderElector elector;

	public OverseerElectionContext2(ZooKeeper zkClient, Overseer overseer,
			final String zkNodeName, LeaderElector elector) {
		super(zkNodeName, "/overseer_elect", "/overseer_elect/leader", null,
				zkClient);
		this.overseer = overseer;
		this.zkClient = zkClient;
		this.elector = elector;
	}

	void rejoinLeaderElection() throws KeeperException, InterruptedException,
			IOException {
		cancelElection();
		elector.joinElection(this, true);
	}

	@Override
	void runLeaderProcess(boolean weAreReplacement) throws KeeperException,
			InterruptedException {

		final String id = leaderSeqPath.substring(leaderSeqPath
				.lastIndexOf("/") + 1);
		ZkNodeProps myProps = new ZkNodeProps("id", id);

		try {
			new ZookeeperClient(zkClient).makePath(leaderPath, ZkStateReader.toJSON(myProps),
					CreateMode.EPHEMERAL);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		overseer.start(id);
		
		
	}

}
