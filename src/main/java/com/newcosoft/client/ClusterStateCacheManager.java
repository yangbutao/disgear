package com.newcosoft.client;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.newcosoft.zookeeper.ClusterState;

public class ClusterStateCacheManager {
	public static ClusterStateCacheManager INSTANCE=new ClusterStateCacheManager();
	private ClusterState clusterState;
	public static final String CLUSTER_STATE = "/clusterstate.json";
	public static final String LIVE_NODES_ZKNODE = "/live_nodes";
	private ZooKeeper zkClient;
	private AtomicInteger totals = new AtomicInteger(100);

	public static void main(String[] args) {
		ClusterStateCacheManager manager = new ClusterStateCacheManager();
		try {
			manager.createClusterStateWatcher();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public AtomicInteger getTotals() {
		return totals;
	}

	public void setTotals(AtomicInteger totals) {
		this.totals = totals;
	}

	public ClusterState getClusterState() {
		return clusterState;
	}

	public void setClusterState(ClusterState clusterState) {
		this.clusterState = clusterState;
	}

	public ZooKeeper getZkClient() {
		return zkClient;
	}

	public void setZkClient(ZooKeeper zkClient) {
		this.zkClient = zkClient;
	}

	public ClusterStateCacheManager() {

	}

	public void init(String zkUrl) {
		try {
			this.zkClient = new ZooKeeper(zkUrl, 100000, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * monitor to clusterstate.json
	 */
	public void createClusterStateWatcher() throws Exception {

		byte[] datas = zkClient.getData(CLUSTER_STATE, new Watcher() {

			public void process(WatchedEvent event) {
				if (EventType.None.equals(event.getType())) {
					return;
				}

				try {

					synchronized (this) {
						final Watcher thisWatch = this;
						Stat stat = new Stat();
						byte[] data = zkClient.getData(CLUSTER_STATE,
								thisWatch, stat);
						List<String> liveNodes = zkClient.getChildren(
								LIVE_NODES_ZKNODE, this);

						Set<String> liveNodesSet = new HashSet<String>();
						liveNodesSet.addAll(liveNodes);
						Set<String> ln = clusterState.getLiveNodes();
						ClusterState newClusterState = ClusterState.load(
								stat.getVersion(), data, liveNodesSet);
						clusterState = newClusterState;
						CacheOperation.INSTANCE.clearJRedisPool();
						System.out.println("CLUSTER_STATE changed");
						System.out.println(clusterState.toString());

					}
				} catch (KeeperException e) {
					if (e.code() == KeeperException.Code.SESSIONEXPIRED
							|| e.code() == KeeperException.Code.CONNECTIONLOSS) {
						return;
					}
					// log.error("", e);
					throw new RuntimeException(e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}

		}, null);
		Stat stat = new Stat();
		List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE, null);
		Set<String> liveNodesSet = new HashSet<String>();
		liveNodesSet.addAll(liveNodes);
		clusterState = ClusterState
				.load(stat.getVersion(), datas, liveNodesSet);
	}

}
