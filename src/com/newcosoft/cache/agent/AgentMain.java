package com.newcosoft.cache.agent;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.newcosoft.cache.CacheCollection;
import com.newcosoft.cache.Shard;
import com.newcosoft.zookeeper.ClusterState;
import com.newcosoft.zookeeper.CollectionContainer;

public class AgentMain {

	private String shardName;
	private String colName;
	private String nodeName;
	private String leaderName;
	private List<String> replicas = new ArrayList<String>();
	private CollectionContainer cc;
	public static AgentMain INSTANCE = new AgentMain();

	/**
	 * 代理启动的main类 在redis启动完成后，启动agent，完成和zookeeper的连接 后ping
	 * redis,监控是否存活，如果超时，则更新zookeeper上的节点信息
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// -Dcol=xx -Dshard=xx -Dnode=xx -DbaseUrl=xx -DscriptPath=xx -DshardNum=2 -Dzk_url=10.1.1.25:2181

		String col = null;
		String shard = null;
		String nodeName = null;
		String baseUrl = null;
		String scriptPath=null;
		String zk_url=null;
		int shardNum=0;
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("-D")) {
				String[] tmp = args[i].split("=");
				if (tmp[0].equals("-Dcol")) {
					col = tmp[1];
				} else if (tmp[0].equals("-Dshard")) {
					shard = tmp[1];
				} else if (tmp[0].equals("-Dnode")) {
					nodeName = tmp[1];
				} else if (tmp[0].equals("-DbaseUrl")) {
					baseUrl = tmp[1];
				}else if(tmp[0].equals("-DscriptPath")){
					scriptPath=tmp[1];
				}else if(tmp[0].equals("-DshardNum")){
					shardNum=new Integer(tmp[1]);
				}else if(tmp[0].equals("-Dzk_url")){
					zk_url=tmp[1];
				}
			}
		}
		INSTANCE.shardName = shard;
		INSTANCE.nodeName = nodeName;
		CollectionContainer cc = new CollectionContainer(col, shard, nodeName,
				baseUrl, shardNum,zk_url,scriptPath);
		// INSTANCE.cc=cc;
		cc.load();
		// ping redis

		String monitorScriptPath = scriptPath+File.separator+"monitor-redis.sh";
		String result = null;
		try {
			Thread.currentThread().sleep(3000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (true) {
			result = ShellExec.runExec(monitorScriptPath);
			// result = "PONG";
			if (result == null || !"PONG".equals(result)) {
				cc.getZkController().clearNodeStatus(nodeName);
			} else {
				if (cc.getZkController().getLeaderElectPath() == null) {
					cc.getZkController().rejoinLeaderElection(nodeName);
				}
			}
			/*
			 * if(result==null){ result="PONG"; }else{ result=null; }
			 */

			try {
				Thread.currentThread().sleep(3000);
			} catch (InterruptedException e) { // TODO Auto-generated catch
												// block e.printStackTrace();
			}
		}

	}

	public String getShardName() {
		return shardName;
	}

	public void setShardName(String shardName) {
		this.shardName = shardName;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getLeaderName() {
		return leaderName;
	}

	public void setLeaderName(String leaderName) {
		this.leaderName = leaderName;
	}

	public List<String> getReplicas() {
		return replicas;
	}

	public void setReplicas(List<String> replicas) {
		this.replicas = replicas;
	}

	public CollectionContainer getCc() {
		return cc;
	}

	public void setCc(CollectionContainer cc) {
		this.cc = cc;
	}

	/**
	 * 根据ClusterState的变更，更新缓存的本地master-slave角色
	 */
	public void doIfRedisRolechanged(ClusterState oldClusterState,
			ClusterState newClusterState) {
		for (CacheCollection coll : oldClusterState.getCollectionStates()
				.values()) {
			String currCol = coll.getName();
			for (Shard slice : coll.getShards()) {
				if (slice.getName().equals(AgentMain.INSTANCE.getShardName())) {
					String oldLeaderName = (String) slice.getLeader().get(
							"node_name");

					Shard shard = newClusterState.getCollection(currCol)
							.getShard(AgentMain.INSTANCE.getShardName());
					String newLeaderName = shard.getLeader()
							.getStr("node_name");

					if ((leaderName == null && newLeaderName != null)
							|| !leaderName.equals(newLeaderName)) {
						leaderName = newLeaderName;
						// ShellExec.runExec("/opt/lsmp/redis_slave.sh");
					}

				}

			}
		}

	}
}
