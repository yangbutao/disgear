package com.newcosoft.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class CacheOperation {
	private ConcurrentHashMap<String, JedisPool> jredisPoolMap;
	public static CacheOperation INSTANCE = new CacheOperation();
	private static ConcurrentHashMap<String, AtomicLong> count = new ConcurrentHashMap<String, AtomicLong>();

	public void insert(String host, int port, String key,
			Map<String, String> hash) {
		if (jredisPoolMap == null) {
			jredisPoolMap = new ConcurrentHashMap<String, JedisPool>();
		}
		JedisPool pool = jredisPoolMap.get(host + ":" + port);
		if (pool == null) {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxActive(100);
			config.setMaxIdle(20);
			config.setMaxWait(1000l);
			pool = new JedisPool(host, port);
			jredisPoolMap.put(host + ":" + port, pool);
		}
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.hmset(key, hash);
		} finally {
			pool.returnResource(jedis);
		}
	}
	
	
	public void insert(String host, int port, String key,
			String mapKey,String mapValue) {
		if (jredisPoolMap == null) {
			jredisPoolMap = new ConcurrentHashMap<String, JedisPool>();
		}
		JedisPool pool = jredisPoolMap.get(host + ":" + port);
		if (pool == null) {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxActive(100);
			config.setMaxIdle(20);
			config.setMaxWait(1000l);
			pool = new JedisPool(host, port);
			jredisPoolMap.put(host + ":" + port, pool);
		}
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.hset(key, mapKey, mapValue);
		} finally {
			pool.returnResource(jedis);
		}
	}

	public Map<String, String> read(String host, int port, String key) throws Exception {
		// host = "10.1.1.26";
		if (jredisPoolMap == null) {
			jredisPoolMap = new ConcurrentHashMap<String, JedisPool>();
		}
		JedisPool pool = jredisPoolMap.get(host + ":" + port);
		if (pool == null) {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxActive(100);
			config.setMaxIdle(20);
			config.setMaxWait(1000l);
			pool = new JedisPool(host, port);
			jredisPoolMap.put(host + ":" + port, pool);
		}

		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			Map<String, String> user = jedis.hgetAll(key);
			// for test
			/*
			 * AtomicLong tmp = count.get(host); if (tmp == null) { tmp = new
			 * AtomicLong(0); } tmp.addAndGet(1); count.put(host, tmp);
			 */
			return user;
			// System.out.println("********************"+count.toString());
		} finally {
			pool.returnResource(jedis);
		}

	}
	
	public String read(String host, int port, String key,String mapKey) throws Exception {
		// host = "10.1.1.26";
		if (jredisPoolMap == null) {
			jredisPoolMap = new ConcurrentHashMap<String, JedisPool>();
		}
		JedisPool pool = jredisPoolMap.get(host + ":" + port);
		if (pool == null) {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxActive(100);
			config.setMaxIdle(20);
			config.setMaxWait(1000l);
			pool = new JedisPool(host, port);
			jredisPoolMap.put(host + ":" + port, pool);
		}

		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.hget(key,mapKey);
			// for test
			/*
			 * AtomicLong tmp = count.get(host); if (tmp == null) { tmp = new
			 * AtomicLong(0); } tmp.addAndGet(1); count.put(host, tmp);
			 */
			//return user;
			// System.out.println("********************"+count.toString());
		} finally {
			pool.returnResource(jedis);
		}

	}
	

	public void clearJRedisPool() {
		for (JedisPool pool : jredisPoolMap.values()) {
			pool.destroy();
		}
		jredisPoolMap.clear();
	}

}
