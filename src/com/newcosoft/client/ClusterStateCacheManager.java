package com.newcosoft.client;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.newcosoft.zookeeper.ZkStateReader;

public class ClusterStateCacheManager {
	
	public static void main(String[] args) {
		ClusterStateCacheManager manager=new ClusterStateCacheManager();
		manager.createClusterStateWatcher();

	}

	/**
	 * 客户端对clusterstate.json节点的监控
	 */
	public void createClusterStateWatcher(){
	  try {
		  ZooKeeper zkClient = new ZooKeeper("10.1.1.25", 100000, null);
		  ZkStateReader reader=new ZkStateReader(zkClient,null);
		  try {
			reader.createClusterStateWatchersAndUpdate();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  while(true){
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
