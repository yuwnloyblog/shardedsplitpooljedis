package com.yuwnloy.shardedsplitpooljedis;

import java.util.concurrent.ConcurrentHashMap;

import redis.clients.jedis.BinaryShardedJedis;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ShardedJedisPipeline;

public class ShardedSplitPoolJedisPipeline extends ShardedJedisPipeline {
	private BinaryShardedSplitPoolJedis jedisPool;
	private ConcurrentHashMap<String, Jedis_JedisPool> jedisMap = new ConcurrentHashMap<String, Jedis_JedisPool>();

	@Deprecated
	public final void setShardedJedis(BinaryShardedJedis jedis) {
	}

	public void setShardedPoolJedis(BinaryShardedSplitPoolJedis jedisPool) {
		this.jedisPool = jedisPool;
	}

//	@Override
//	protected Client fetchClientFromPool(String key){
//		JedisPool jedisPool = this.jedisPool.getShard(key);
//		JedisShardPoolInfo shardInfo = this.jedisPool.getShardInfo(key);
//		if(!jedisMap.containsKey(shardInfo.toString()))
//			jedisMap.putIfAbsent(shardInfo.toString(), new Jedis_JedisPool(jedisPool.getResource(),jedisPool));
//		Jedis jedis = this.jedisMap.get(shardInfo.toString()).getJedis();
//    	return jedis.getClient();
//    }
	
	@Override
	 public void sync() {
		super.sync();
		for(Jedis_JedisPool jedis_jedisPoolPair : this.jedisMap.values()){
			jedis_jedisPoolPair.releaseJedis();
		}
		this.jedisMap.clear();
	}
	private static class Jedis_JedisPool{
		private Jedis jedis;
		private JedisPool pool;
		public Jedis_JedisPool(Jedis jedis, JedisPool pool){
			this.jedis = jedis;
			this.pool = pool;
		}
		public Jedis getJedis(){
			return this.jedis;
		}
		public void releaseJedis(){
			this.pool.returnResource(this.jedis);
		}
	}
}
