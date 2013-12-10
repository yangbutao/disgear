package com.newcosoft.cache.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.newcosoft.cache.CacheCollection;
import com.newcosoft.cache.Range;
import com.newcosoft.cache.Replica;
import com.newcosoft.cache.Router;
import com.newcosoft.cache.Shard;

public class TestShard {
	public static void main1(String[] args) {
		/*List<Range> ranges1 = Router.DEFAULT.partitionRange(2);
		for (Range range : ranges1) {
			System.out.println(range.toString());
		}*/

		

		List<Range> ranges= Router.DEFAULT.partitionRange(3);

		Replica replica1=new Replica("node1", new HashMap());
		Replica replica11=new Replica("node11", new HashMap());
		Map<String,Replica> replicas_1=new HashMap<String, Replica>();
		replicas_1.put("replica1", replica1);
		replicas_1.put("replica11", replica11);
		Map<String, Object> propMap1=new HashMap<String, Object>();
		propMap1.put("range", ranges.get(0));
		Shard shard1=new Shard("shard1", replicas_1, propMap1);
		
		Replica replica2=new Replica("node2", new HashMap());
		Replica replica21=new Replica("node21", new HashMap());
		Map<String,Replica> replicas_2=new HashMap<String, Replica>();
		replicas_2.put("replica2", replica2);
		replicas_2.put("replica21", replica21);
		Map<String, Object> propMap2=new HashMap<String, Object>();
		propMap2.put("range", ranges.get(1));
		Shard shard2=new Shard("shard2", replicas_2, propMap2);
		

		Replica replica3=new Replica("node3", new HashMap());
		Replica replica31=new Replica("node31", new HashMap());
		Map<String,Replica> replicas_3=new HashMap<String, Replica>();
		replicas_3.put("replica3", replica3);
		replicas_3.put("replica31", replica31);
		Map<String, Object> propMap3=new HashMap<String, Object>();
		propMap3.put("range", ranges.get(2));
		Shard shard3=new Shard("shard3", replicas_3, propMap3);
		
		
		
		
		Map<String, Shard> shards=new HashMap<String, Shard>();
		shards.put("shard1", shard1);
		shards.put("shard2", shard2);
		shards.put("shard3", shard3);
		CacheCollection col=new CacheCollection("testCol",shards,new HashMap(),Router.DEFAULT);
		
		System.out.println("*****************uuuuudoo1**********"+Router.DEFAULT.getTargetShard("uuuuudoo5", col).getName());
		System.out.println("*****************uuuuudoo2**********"+Router.DEFAULT.getTargetShard("423423468913", col).getName());
		System.out.println("*****************uuuuudoo3**********"+Router.DEFAULT.getTargetShard("423423468925", col).getName());
	}
	
	public static void main(String[] args) {
		List<Range> ranges= Router.DEFAULT.partitionRange(3);
		Replica replica1=new Replica("node1", new HashMap());
		Replica replica11=new Replica("node11", new HashMap());
		Map<String,Replica> replicas_1=new HashMap<String, Replica>();
		replicas_1.put("replica1", replica1);
		replicas_1.put("replica11", replica11);
		Map<String, Object> propMap1=new HashMap<String, Object>();
		propMap1.put("range", ranges.get(0));
		Shard shard1=new Shard("shard1", replicas_1, propMap1);
		
		Replica replica2=new Replica("node2", new HashMap());
		Replica replica21=new Replica("node21", new HashMap());
		Map<String,Replica> replicas_2=new HashMap<String, Replica>();
		replicas_2.put("replica2", replica2);
		replicas_2.put("replica21", replica21);
		Map<String, Object> propMap2=new HashMap<String, Object>();
		propMap2.put("range", ranges.get(1));
		Shard shard2=new Shard("shard2", replicas_2, propMap2);
		

		Replica replica3=new Replica("node3", new HashMap());
		Replica replica31=new Replica("node31", new HashMap());
		Map<String,Replica> replicas_3=new HashMap<String, Replica>();
		replicas_3.put("replica3", replica3);
		replicas_3.put("replica31", replica31);
		Map<String, Object> propMap3=new HashMap<String, Object>();
		propMap3.put("range", ranges.get(2));
		Shard shard3=new Shard("shard3", replicas_3, propMap3);
		
		
		
		
		Map<String, Shard> shards=new HashMap<String, Shard>();
		shards.put("shard1", shard1);
		shards.put("shard2", shard2);
		shards.put("shard3", shard3);
		CacheCollection col=new CacheCollection("testCol",shards,new HashMap(),Router.DEFAULT);
		
		int shard1Count=0;
		int shard2Count=0;
		int shard3Count=0;
		for(int i=1;i<9999999;i++){
			String key="0000000000"+i;
			String shardName=Router.DEFAULT.getTargetShard(key, col).getName();
			if(shardName.equals("shard1")){
				shard1Count++;
			}else if(shardName.equals("shard2")){
				shard2Count++;
			}
			else if(shardName.equals("shard3")){
				shard3Count++;
			}
		}
		
		System.out.println("shard1="+shard1Count);
		System.out.println("shard2="+shard2Count);
		System.out.println("shard3="+shard3Count);
		/*
		System.out.println("*****************uuuuudoo1**********"+Router.DEFAULT.getTargetShard("uuuuudoo5", col).getName());
		System.out.println("*****************uuuuudoo2**********"+Router.DEFAULT.getTargetShard("423423468913", col).getName());
		System.out.println("*****************uuuuudoo3**********"+Router.DEFAULT.getTargetShard("423423468925", col).getName());
*/	}
	

}
