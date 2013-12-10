package com.newcosoft.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.newcosoft.cache.Replica;
import com.newcosoft.cache.Router;
import com.newcosoft.cache.Shard;
import com.newcosoft.zookeeper.ClusterState;

public class CacheClient {
	private static Logger log = LoggerFactory.getLogger(CacheClient.class);

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

	public static void main(String[] args) throws Exception {
		try {
			ClusterStateCacheManager.INSTANCE.init("10.1.1.25");
			ClusterStateCacheManager.INSTANCE.createClusterStateWatcher();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String key = "000000000000000005";
		queryByKey(key, 10);
		// CacheClient.INSTANCE.read(urls[0], new Integer(urls[1]), key);
		queryByKey(key, 10);
	}

	/**
	 * write data to redis
	 * 
	 * @param key
	 *            redis key
	 * @param hash
	 *            value of redis,type is HashMap
	 */
	public static void writeByKey(String key, Map<String, String> hash) {
		writeByKey(key, hash, 10);
	}

	/**
	 * write data to redis
	 * 
	 * @param key
	 *            redis key
	 * @param mapKey
	 *            the value map key
	 * @param mapValue
	 *            the value map value
	 */
	public static void writeByKey(String key, String mapKey, String mapValue) {
		writeByKey(key, mapKey, mapValue, 10);
	}

	public static void writeByKey(String key, String mapKey, String mapValue,
			int tryCnt) {
		String writerUrl = null;
		try {
			writerUrl = getWriteUrl(key);
			String[] urls = writerUrl.split(":");
			CacheOperation.INSTANCE.insert(urls[0], new Integer(urls[1]), key,
					mapKey, mapValue);
		} catch (Exception e) {
			System.out.println("*******writerUrl********" + writerUrl);
			e.printStackTrace();
			tryCnt--;
			if (tryCnt > 0) {
				writeByKey(key, mapKey, mapValue, tryCnt);
			}
		}
	}

	/**
	 * for test
	 * 
	 * @param key
	 */
	public static void writeByKey(String key, Map<String, String> hash,
			int tryCnt) {
		String writerUrl = null;
		try {
			writerUrl = getWriteUrl(key);
			String[] urls = writerUrl.split(":");
			/*
			 * Map<String, String> hash = new HashMap<String, String>();
			 * hash.put("userNum", key); hash.put("userId", key);
			 * hash.put("userName", key); hash.put("userProvinceNum", "01");
			 * hash.put("userLevel", "11"); hash.put("telephone",
			 * "13916351796"); hash.put("email", "yangbutao@hotmail.com");
			 * hash.put("errorLoginCnt", new Integer(10).toString());
			 * hash.put("lastLoginTime", new Long(10993840088L).toString());
			 * hash.put("cityNum", "1900"); hash.put("group", "02");
			 * hash.put("realName", key); hash.put("area", "china shanghai");
			 * hash.put("lastLoginIp", "198.33.23.11");
			 */
			CacheOperation.INSTANCE.insert(urls[0], new Integer(urls[1]), key,
					hash);
		} catch (Exception e) {
			System.out.println("*******writerUrl********" + writerUrl);
			e.printStackTrace();
			tryCnt--;
			if (tryCnt > 0) {
				writeByKey(key, hash, tryCnt);
			}
		}
	}

	public static void main3(String[] args) {
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

	/**
	 * query data by key
	 * 
	 * @param key
	 *            redis key
	 */
	public static Map<String, String> queryByKey(String key) {
		return queryByKey(key, 10);
	}

	/**
	 * 
	 * @param key
	 *            redis key
	 * @param mapKey
	 *            the value Map key
	 * @return
	 */
	public static String queryByKey(String key, String mapKey) {
		return queryByKey(key, mapKey, 10);
	}

	/**
	 * 
	 * @param key
	 *            redis key
	 * @param mapKey
	 *            the value Map key
	 * @return
	 */
	public static String queryByKey(String key, String mapKey, int tryCnt) {
		String readerUrl = getReaderUrl(key);
		String[] urls = readerUrl.split(":");
		try {
			return CacheOperation.INSTANCE.read(urls[0], new Integer(urls[1]),
					key, mapKey);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("**********readUrl*****" + readerUrl);
			// log.error("query by key error ",e);
			e.printStackTrace();
			tryCnt--;
			if (tryCnt > 0) {
				return queryByKey(key, mapKey, tryCnt);
			}
		}
		return null;

	}

	/**
	 * for test
	 * 
	 * @param key
	 */
	public static Map<String, String> queryByKey(String key, int tryCnt) {

		String readerUrl = getReaderUrl(key);
		String[] urls = readerUrl.split(":");
		Map<String, String> ret = null;
		try {
			ret = CacheOperation.INSTANCE.read(urls[0], new Integer(urls[1]),
					key);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("**********readUrl*****" + readerUrl);
			// log.error("query by key error ",e);
			e.printStackTrace();
			tryCnt--;
			if (tryCnt > 0) {
				return queryByKey(key, tryCnt);
			}
		}

		return ret;

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

					String writeUrl = getWriteUrl(key);
					String[] urls = writeUrl.split(":");
					CacheOperation.INSTANCE.insert(urls[0],
							new Integer(urls[1]), key, hash);

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

	public static String getWriteUrl(String key) {
		ClusterState clusterState = ClusterStateCacheManager.INSTANCE
				.getClusterState();
		Set<String> liveNodes = clusterState.getLiveNodes();
		Shard shard = Router.DEFAULT.getTargetShard(key,
				clusterState.getCollection("DEFAULT_COL"));
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

	public static String getReaderUrl(String key) {
		// String key = "000000000000000001";
		ClusterState clusterState = ClusterStateCacheManager.INSTANCE
				.getClusterState();
		Set<String> liveNodes = clusterState.getLiveNodes();
		Shard shard = Router.DEFAULT.getTargetShard(key,
				clusterState.getCollection("DEFAULT_COL"));
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
		// System.out.println("***********index size="+index+"************replicas="+replicas.length);
		Replica reader = replicas[index < 0 ? (index + 1) : index];
		while (!liveNodes.contains(reader.getName())) {
			totals = ClusterStateCacheManager.INSTANCE.getTotals()
					.getAndDecrement();
			if (totals <= 0) {
				ClusterStateCacheManager.INSTANCE.setTotals(new AtomicInteger(
						10000));
			}
			index = totals % (replicas.length);
			reader = replicas[index < 0 ? (index + 1) : index];
		}
		return reader.getStr("base_url");
	}
}
