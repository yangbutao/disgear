package com.newcosoft.client;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.newcosoft.zookeeper.ZkStateReader;

public class ClusterStateCacheManager {
	private static ClusterStateCacheManager INSTANCE = new ClusterStateCacheManager();
	private ZkStateReader reader;

	public static void main(String[] args) {
		String zkUrl = args[0];
		ClusterStateCacheManager manager = new ClusterStateCacheManager();
		manager.createClusterStateWatcher(zkUrl);

	}

	public static ClusterStateCacheManager getINSTANCE() {
		return INSTANCE;
	}

	public static void setINSTANCE(ClusterStateCacheManager iNSTANCE) {
		INSTANCE = iNSTANCE;
	}

	/**
	 * the monitor of client clusterstate.json to clusterstate.json
	 */
	public void createClusterStateWatcher(String zkUrl) {
		try {
			ZooKeeper zkClient = new ZooKeeper(zkUrl, 100000, null);
			reader = new ZkStateReader(zkClient, null);
			try {
				reader.createClusterStateWatchersAndUpdate();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			while (true) {
				System.out.println(reader.getClusterState().toString());
				try {
					Thread.currentThread().sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
