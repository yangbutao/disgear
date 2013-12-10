package com.newcosoft.zookeeper;

import com.newcosoft.cache.agent.AgentMain;

public class CollectionContainer {

	private ZookeeperController zkController;
	private String collectionName;
	private String shardId;
	private String nodeName;
	private int numShards;
	private String baseUrl;
	private String zkUrl;
	private String scriptPath;
	

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

	public int getNumShards() {
		return numShards;
	}

	public void setNumShards(int numShards) {
		this.numShards = numShards;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getShardId() {
		return shardId;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	
	public String getZkUrl() {
		return zkUrl;
	}

	public void setZkUrl(String zkUrl) {
		this.zkUrl = zkUrl;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public CollectionContainer(String collectionName, String shardId, String nodeName,String baseUrl,int numShards,String zkUrl,String scriptPath) {
		this.collectionName=collectionName;
		this.shardId=shardId;
		this.nodeName=nodeName;
		this.numShards=numShards;
		this.baseUrl=baseUrl;
		this.zkUrl=zkUrl;
		this.scriptPath=scriptPath;
		AgentMain.INSTANCE.setCc(this);
		// create Observer
		zkController = new ZookeeperController(zkUrl, 30000,this);
	}

	public void load() {
		CollectionDesc desc = new CollectionDesc();
		desc.setCollectionName(collectionName);
		desc.setCoreNodeName(nodeName);
		desc.setShardId(shardId);
		desc.setNumShards(numShards);
		zkController.setNodeName(nodeName);
		zkController.setBaseUrl(baseUrl);
		zkController.preRegister(desc);
		// create collection process
		zkController.createCollection(desc);
		// register shard,enter vote for leader process
		try {
			zkController.registerInZk(desc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			Thread.currentThread().sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(zkController.getZkStateReader().getClusterState().toString());
		
	}

	public ZookeeperController getZkController() {
		return zkController;
	}

	public void setZkController(ZookeeperController zkController) {
		this.zkController = zkController;
	}
	

}
