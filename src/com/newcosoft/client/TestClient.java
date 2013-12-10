package com.newcosoft.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.newcosoft.cache.Replica;
import com.newcosoft.cache.Router;
import com.newcosoft.cache.Shard;
import com.newcosoft.zookeeper.ClusterState;

public class TestClient {
	public static void main1(String[] args) {
		try {
			ClusterStateCacheManager.INSTANCE.createClusterStateWatcher();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println(ClusterStateCacheManager.INSTANCE.getClusterState());
		loadIntoRedis();
		try {
			Thread.currentThread().sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main2(String[] args) throws Exception {
		try {
			ClusterStateCacheManager.INSTANCE.createClusterStateWatcher();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String key = "000000000000000001";
		String readerUrl = getReaderUrl("col1", key);
		String[] urls = readerUrl.split(":");
		CacheClient.INSTANCE.read(urls[0], new Integer(urls[1]), key);
		System.out.println(ClusterStateCacheManager.INSTANCE.getClusterState());

	}
	
	public static void writeByKey(String key){
		writeByKey(key,10);
	}

	/**
	 * for test
	 * 
	 * @param key
	 */
	public static void writeByKey(String key, int tryCnt) {
		String writerUrl=null;
		try {
			writerUrl = getWriteUrl("col1", key);
			String[] urls = writerUrl.split(":");
			Map<String, String> hash = new HashMap<String, String>();
			hash.put("userNum", key);
			hash.put("userId", key);
			hash.put("userName", key);
			hash.put("userProvinceNum", "01");
			hash.put("userLevel", "11");
			hash.put("telephone", "13916351796");
			hash.put("email", "yangbutao@hotmail.com");
			hash.put("errorLoginCnt", new Integer(10).toString());
			hash.put("lastLoginTime", new Long(10993840088L).toString());
			hash.put("cityNum", "1900");
			hash.put("group", "02");
			hash.put("realName", key);
			hash.put("area", "china shanghai");
			hash.put("lastLoginIp", "198.33.23.11");
			CacheClient.INSTANCE.insert(urls[0], new Integer(urls[1]), key,
					hash);
		} catch (Exception e) {
			System.out.println("*******writerUrl********"+writerUrl);
			e.printStackTrace();
			tryCnt--;
			if (tryCnt > 0) {
				writeByKey(key,tryCnt);
			}
		}
	}

	public static void main(String[] args) {
		String key = "x";
		int num = new java.util.Random().nextInt(999999);
		String numStr = new java.lang.Integer(num).toString();
		if (numStr.length() < 8) {
			int xx = 8 - numStr.length();
			while (xx > 0) {
				key = key + "0";
				xx--;
			}
		}
		key = key + numStr;
		System.out.println(key.length());
		System.out.println("***" + num + "****" + key);
	}

	
	public static void queryByKey(String key){
		queryByKey(key, 10);
	}
	/**
	 * for test
	 * 
	 * @param key
	 */
	public static void queryByKey(String key, int tryCnt) {

		String readerUrl = getReaderUrl("col1", key);
		String[] urls = readerUrl.split(":");
		try {
			CacheClient.INSTANCE.read(urls[0], new Integer(urls[1]), key);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("**********readUrl*****"+readerUrl);
			e.printStackTrace();
			tryCnt--;
			if (tryCnt > 0) {
				queryByKey(key, tryCnt);
			}
		}

	}

	public static void loadIntoRedis() {
		BoneCP connectionPool = null;

		Connection connection = null;
		try {
			// load the database driver (make sure this is in your classpath!)
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		try {
			// setup the connection pool
			BoneCPConfig config = null;
			try {
				config = new BoneCPConfig("bonecp-config.xml");
			} catch (Exception e) {
				e.printStackTrace();
			}
			connectionPool = new BoneCP(config); // setup the connection pool

			long startTime = System.currentTimeMillis();
			connection = connectionPool.getConnection(); // fetch a
															// connection
			if (connection != null) {
				System.out.println("Connection successful!");
				Statement stmt = connection.createStatement();
				ResultSet rs = stmt
						.executeQuery("select * from lsmp_lottery_user");

				while (rs.next()) {
					String key = rs.getString("LUSER_NUM");
					String userName = rs.getString("LUSER_NAME");
					String userId = rs.getString("ID");
					String userProvinceNum = rs.getString("LUSER_PROVINCE_NUM");
					String userLevel = rs.getString("LUSER_LEVEL");
					String telephone = rs.getString("LUSER_TELEPHONE");
					String email = "yangbutao@newcosoft.com";
					int errorLoginCnt = 10;
					long lastLoginTime = 1988922384994L;
					String cityNum = rs.getString("LUSER_CITY_NUM");
					String group = "1";
					String realName = rs.getString("LUSER_REAL_NAME");
					String area = "xxxxxxxxxxxxxxxxxx";
					String lastLoginIp = "109.222.44.32";
					Map<String, String> hash = new HashMap<String, String>();
					hash.put("userNum", key);
					hash.put("userId", userId);
					hash.put("userName", userName);
					hash.put("userProvinceNum", userProvinceNum);
					hash.put("userLevel", userLevel);
					hash.put("telephone", telephone);
					hash.put("email", email);
					hash.put("errorLoginCnt",
							new Integer(errorLoginCnt).toString());
					hash.put("lastLoginTime",
							new Long(lastLoginTime).toString());
					hash.put("cityNum", cityNum);
					hash.put("group", group);
					hash.put("realName", realName);
					hash.put("area", area);
					hash.put("lastLoginIp", lastLoginIp);

					String writeUrl = getWriteUrl("col1", key);
					String[] urls = writeUrl.split(":");
					CacheClient.INSTANCE.insert(urls[0], new Integer(urls[1]),
							key, hash);

				}
			}
			connectionPool.shutdown(); // shutdown connection pool.
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static String getWriteUrl(String col, String key) {
		ClusterState clusterState = ClusterStateCacheManager.INSTANCE
				.getClusterState();
		Set<String> liveNodes = clusterState.getLiveNodes();
		Shard shard = Router.DEFAULT.getTargetShard(key,
				clusterState.getCollection("col1"));
		Replica leader = shard.getLeader();
		while (!liveNodes.contains(leader.getName())) {
			try {
				Thread.currentThread().sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			leader = shard.getLeader();
		}
		return leader.getStr("base_url");
	}

	public static String getReaderUrl(String col, String key) {
		// String key = "000000000000000001";
		ClusterState clusterState = ClusterStateCacheManager.INSTANCE
				.getClusterState();
		Set<String> liveNodes = clusterState.getLiveNodes();
		Shard shard = Router.DEFAULT.getTargetShard(key,
				clusterState.getCollection("col1"));
		// for read
		// roundrobin

		Replica[] replicas = shard.getReplicas().toArray(
				new Replica[shard.getReplicas().size()]);

		int totals = ClusterStateCacheManager.INSTANCE.getTotals()
				.getAndDecrement();
		if (totals <= 0) {
			ClusterStateCacheManager.INSTANCE
					.setTotals(new AtomicInteger(10000));
		}
		int index = totals % (replicas.length);
		Replica reader = replicas[index];
		while (!liveNodes.contains(reader.getName())) {
			totals = ClusterStateCacheManager.INSTANCE.getTotals()
					.getAndDecrement();
			if (totals <= 0) {
				ClusterStateCacheManager.INSTANCE.setTotals(new AtomicInteger(
						10000));
			}
			index = totals % (replicas.length);
			reader = replicas[index];
		}
		return reader.getStr("base_url");
	}
}
