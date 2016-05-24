package com.yuwnloy.shardedsplitpooljedis;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.util.Hashing;

public class ShardedSplitPoolJedis extends BinaryShardedSplitPoolJedis implements JedisCommands {

	public ShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards) {
		super(shards, Hashing.MURMUR_HASH);
	}

	public ShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards, Hashing algo) {
		super(shards, algo);
	}

	public ShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards, Pattern keyTagPattern) {
		super(shards, Hashing.MURMUR_HASH, keyTagPattern);
	}

	public ShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards, Hashing algo, Pattern keyTagPattern) {
		super(shards, algo, keyTagPattern);
	}

	public void destroy() {
		for (JedisPool jedisPool : getAllShards()) {
			jedisPool.destroy();
		}
	}

	/**
	 * just use destroy() to replace.
	 */
	@Deprecated
	public void disconnect() {
		this.destroy();
	}

	@Override
	public String set(final String key, final String value) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis jedis) {
				return jedis.set(key, value);
			}
		}.run();
	}

	@Override
	public String get(final String key) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis jedis) {
				return jedis.get(key);
			}
		}.run();
	}

	@Override
	public Boolean exists(final String key) {
		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis jedis) {
				return jedis.exists(key);
			}
		}.run();
	}

	@Override
	public String type(final String key) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis jedis) {
				return jedis.type(key);
			}
		}.run();
	}

	@Override
	public Long expire(final String key, final int seconds) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis jedis) {
				return jedis.expire(key, seconds);
			}
		}.run();
	}

	@Override
	public Long expireAt(final String key, final long unixTime) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis jedis) {
				return jedis.expireAt(key, unixTime);
			}
		}.run();
	}

	@Override
	public Long ttl(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis jedis) {
				return jedis.ttl(key);
			}
		}.run();
	}

	@Override
	public Boolean setbit(final String key, final long offset, final boolean value) {
		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				Boolean ret = j.setbit(key, offset, value);
				return ret;
			}
		}.run();

	}

	@Override
	public Boolean getbit(final String key, final long offset) {
		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				Boolean ret = j.getbit(key, offset);
				return ret;
			}
		}.run();

	}

	@Override
	public Long setrange(final String key, final long offset, final String value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.setrange(key, offset, value);
				return ret;
			}
		}.run();

	}

	@Override
	public String getrange(final String key, final long startOffset, final long endOffset) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.getrange(key, startOffset, endOffset);
				return ret;
			}
		}.run();
	}

	@Override
	public String getSet(final String key, final String value) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.getSet(key, value);
				return ret;
			}
		}.run();
	}

	@Override
	public Long setnx(final String key, final String value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.setnx(key, value);
				return ret;
			}
		}.run();
	}

	@Override
	public String setex(final String key, final int seconds, final String value) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.setex(key, seconds, value);
				return ret;
			}
		}.run();
	}

	@Override
	public Long decrBy(final String key, final long integer) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.decrBy(key, integer);
				return ret;
			}
		}.run();

	}

	@Override
	public Long decr(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {

				Long ret = j.decr(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long incrBy(final String key, final long integer) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.incrBy(key, integer);
				return ret;
			}
		}.run();
	}

	@Override
	public Long incr(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.incr(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long append(final String key, final String value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.append(key, value);
				return ret;
			}
		}.run();
	}

	@Override
	public String substr(final String key, final int start, final int end) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.substr(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Long hset(final String key, final String field, final String value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hset(key, field, value);
				return ret;
			}
		}.run();
	}

	@Override
	public String hget(final String key, final String field) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.hget(key, field);
				return ret;
			}
		}.run();
	}

	@Override
	public Long hsetnx(final String key, final String field, final String value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hsetnx(key, field, value);
				return ret;
			}
		}.run();
	}

	@Override
	public String hmset(final String key, final Map<String, String> hash) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.hmset(key, hash);
				return ret;
			}
		}.run();
	}

	@Override
	public List<String> hmget(final String key, final String... fields) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				List<String> ret = j.hmget(key, fields);
				return ret;
			}
		}.run();
	}

	@Override
	public Long hincrBy(final String key, final String field, final long value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hincrBy(key, field, value);
				return ret;
			}
		}.run();
	}

	@Override
	public Boolean hexists(final String key, final String field) {
		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				Boolean ret = j.hexists(key, field);
				return ret;
			}
		}.run();
	}

	public Long del(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.del(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long hdel(final String key, final String... field) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hdel(key, field);
				return ret;
			}
		}.run();
	}

	@Override
	public Long hlen(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hlen(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> hkeys(final String key) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.hkeys(key);
				return ret;
			}
		}.run();
	}

	@Override
	public List<String> hvals(final String key) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				List<String> ret = j.hvals(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Map<String, String> hgetAll(final String key) {
		return new JedisHandler<Map<String, String>>(this.getShard(key)) {
			@Override
			public Map<String, String> execute(Jedis j) {
				Map<String, String> ret = j.hgetAll(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long rpush(final String key, final String... string) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.rpush(key, string);
				return ret;
			}
		}.run();
	}

	@Override
	public Long lpush(final String key, final String... string) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.lpush(key, string);
				return ret;
			}
		}.run();
	}

	@Override
	public Long llen(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.llen(key);
				return ret;
			}
		}.run();
	}

	@Override
	public List<String> lrange(final String key, final long start, final long end) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				List<String> ret = j.lrange(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public String ltrim(final String key, final long start, final long end) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.ltrim(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public String lindex(final String key, final long index) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.lindex(key, index);
				return ret;
			}
		}.run();
	}

	@Override
	public String lset(final String key, final long index, final String value) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.lset(key, index, value);
				return ret;
			}
		}.run();
	}

	@Override
	public Long lrem(final String key, final long count, final String value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.lrem(key, count, value);
				return ret;
			}
		}.run();
	}

	@Override
	public String lpop(final String key) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.lpop(key);
				return ret;
			}
		}.run();
	}

	@Override
	public String rpop(final String key) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.rpop(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long sadd(final String key, final String... member) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.sadd(key, member);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> smembers(final String key) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.smembers(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long srem(final String key, final String... member) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.srem(key, member);
				return ret;
			}
		}.run();
	}

	@Override
	public String spop(final String key) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.spop(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long scard(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.scard(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Boolean sismember(final String key, final String member) {
		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				Boolean ret = j.sismember(key, member);
				return ret;
			}
		}.run();
	}

	@Override
	public String srandmember(final String key) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.srandmember(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zadd(final String key, final double score, final String member) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zadd(key, score, member);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrange(final String key, final long start, final long end) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrange(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zrem(final String key, final String... member) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zrem(key, member);
				return ret;
			}
		}.run();
	}

	@Override
	public Double zincrby(final String key, final double score, final String member) {
		return new JedisHandler<Double>(this.getShard(key)) {
			@Override
			public Double execute(Jedis j) {
				Double ret = j.zincrby(key, score, member);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zrank(final String key, final String member) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zrank(key, member);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zrevrank(final String key, final String member) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zrevrank(key, member);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrevrange(final String key, final long start, final long end) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrevrange(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeWithScores(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeWithScores(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zcard(final String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zcard(key);
				return ret;
			}
		}.run();
	}

	@Override
	public Double zscore(final String key, final String member) {
		return new JedisHandler<Double>(this.getShard(key)) {
			@Override
			public Double execute(Jedis j) {
				Double ret = j.zscore(key, member);
				return ret;
			}
		}.run();
	}

	@Override
	public List<String> sort(final String key) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				List<String> ret = j.sort(key);
				return ret;
			}
		}.run();
	}

	@Override
	public List<String> sort(final String key, final SortingParams sortingParameters) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				List<String> ret = j.sort(key, sortingParameters);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zcount(final String key, final double min, final double max) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zcount(key, min, max);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zcount(final String key, final String min, final String max) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zcount(key, min, max);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrangeByScore(final String key, final double min, final double max) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrangeByScore(key, min, max);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrangeByScore(final String key, final String min, final String max) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrangeByScore(key, min, max);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrevrangeByScore(key, max, min);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset,
			final int count) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrangeByScore(key, min, max, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrevrangeByScore(key, max, min);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset,
			final int count) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrangeByScore(key, min, max, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset,
			final int count) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrevrangeByScore(key, max, min, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeByScoreWithScores(key, max, min);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset,
			final int count) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset,
			final int count) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				Set<String> ret = j.zrevrangeByScore(key, max, min, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeByScoreWithScores(key, max, min);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset,
			final int count) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset,
			final int count) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min, final int offset,
			final int count) {
		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max, offset, count);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zremrangeByRank(final String key, final long start, final long end) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zremrangeByRank(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zremrangeByScore(final String key, final double start, final double end) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zremrangeByScore(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Long zremrangeByScore(final String key, final String start, final String end) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zremrangeByScore(key, start, end);
				return ret;
			}
		}.run();
	}

	@Override
	public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.linsert(key, where, pivot, value);
				return ret;
			}
		}.run();
	}

	@Override
	public Long lpushx(final String key, final String... string) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.lpushx(key, string);
				return ret;
			}
		}.run();
	}

	@Override
	public Long rpushx(final String key, final String... string) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.rpushx(key, string);
				return ret;
			}
		}.run();
	}

	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				return j.set(key, value, nxxx, expx, time);
			}
		}.run();
	}

	@Override
	public String set(String key, String value, String nxxx) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				return j.set(key, value, nxxx);
			}
		}.run();
	}

	@Override
	public Long persist(String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.persist(key);
			}
		}.run();
	}

	@Override
	public Long pexpireAt(String key, long millisecondsTimestamp) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.pexpireAt(key, millisecondsTimestamp);
			}
		}.run();
	}

	@Override
	public Long pttl(String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.pttl(key);
			}
		}.run();
	}

	@Override
	public Boolean setbit(String key, long offset, String value) {
		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				return j.setbit(key, offset, value);
			}
		}.run();
	}

	@Override
	public String psetex(String key, long milliseconds, String value) {
		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				return j.psetex(key, milliseconds, value);
			}
		}.run();
	}

	@Override
	public Double incrByFloat(String key, double value) {
		return new JedisHandler<Double>(this.getShard(key)) {
			@Override
			public Double execute(Jedis j) {
				return j.incrByFloat(key, value);
			}
		}.run();
	}

	@Override
	public Double hincrByFloat(String key, String field, double value) {
		return new JedisHandler<Double>(this.getShard(key)) {
			@Override
			public Double execute(Jedis j) {
				return j.hincrByFloat(key, field, value);
			}
		}.run();
	}

	@Override
	public Set<String> spop(String key, long count) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				return j.spop(key, count);
			}
		}.run();
	}

	@Override
	public List<String> srandmember(String key, int count) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				return j.srandmember(key, count);
			}
		}.run();
	}

	@Override
	public Long strlen(String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.strlen(key);
			}
		}.run();
	}

	@Override
	public Long zadd(String key, double score, String member, ZAddParams params) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.zadd(key, score, member, params);
			}
		}.run();
	}

	@Override
	public Long zadd(String key, Map<String, Double> scoreMembers) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.zadd(key, scoreMembers);
			}
		}.run();
	}

	@Override
	public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.zadd(key, scoreMembers, params);
			}
		}.run();
	}

	@Override
	public Double zincrby(String key, double score, String member, ZIncrByParams params) {
		return new JedisHandler<Double>(this.getShard(key)) {
			@Override
			public Double execute(Jedis j) {
				return j.zincrby(key, score, member, params);
			}
		}.run();
	}

	@Override
	public Long zlexcount(String key, String min, String max) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.zlexcount(key, min, max);
			}
		}.run();
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				return j.zrangeByLex(key, min, max);
			}
		}.run();
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				return j.zrangeByLex(key, min, max, offset, count);
			}
		}.run();
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				return j.zrangeByLex(key, max, min);
			}
		}.run();
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
		return new JedisHandler<Set<String>>(this.getShard(key)) {
			@Override
			public Set<String> execute(Jedis j) {
				return j.zrangeByLex(key, max, min, offset, count);
			}
		}.run();
	}

	@Override
	public Long zremrangeByLex(String key, String min, String max) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.zremrangeByLex(key, min, max);
			}
		}.run();
	}

	@Override
	public List<String> blpop(String arg) {
		return new JedisHandler<List<String>>(this.getShard(arg)) {
			@Override
			public List<String> execute(Jedis j) {
				return j.blpop(arg);
			}
		}.run();
	}

	@Override
	public List<String> blpop(int timeout, String key) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				return j.blpop(timeout, key);
			}
		}.run();
	}

	@Override
	public List<String> brpop(String arg) {
		return new JedisHandler<List<String>>(this.getShard(arg)) {
			@Override
			public List<String> execute(Jedis j) {
				return j.brpop(arg);
			}
		}.run();
	}

	@Override
	public List<String> brpop(int timeout, String key) {
		return new JedisHandler<List<String>>(this.getShard(key)) {
			@Override
			public List<String> execute(Jedis j) {
				return j.brpop(timeout, key);
			}
		}.run();
	}

	@Override
	public String echo(String string) {
		return new JedisHandler<String>(this.getShard(string)) {
			@Override
			public String execute(Jedis j) {
				return j.echo(string);
			}
		}.run();
	}

	@Override
	public Long move(String key, int dbIndex) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.move(key, dbIndex);
			}
		}.run();
	}

	@Override
	public Long bitcount(String key) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.bitcount(key);
			}
		}.run();
	}

	@Override
	public Long bitcount(String key, long start, long end) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.bitcount(key, start, end);
			}
		}.run();
	}

	@Override
	public Long bitpos(String key, boolean value) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.bitpos(key, value);
			}
		}.run();
	}

	@Override
	public Long bitpos(String key, boolean value, BitPosParams params) {
		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				return j.bitpos(key, value, params);
			}
		}.run();
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
		return new JedisHandler<ScanResult<Entry<String, String>>>(this.getShard(key)) {
			@Override
			public ScanResult<Entry<String, String>> execute(Jedis j) {
				return j.hscan(key, cursor);
			}
		}.run();
	}

	@Override
	public ScanResult<String> sscan(String key, int cursor) {
		return new JedisHandler<ScanResult<String>>(this.getShard(key)) {
			@Override
			public ScanResult<String> execute(Jedis j) {
				return j.sscan(key, cursor);
			}
		}.run();
	}

	@Override
	public ScanResult<Tuple> zscan(String key, int cursor) {
		return new JedisHandler<ScanResult<Tuple>>(this.getShard(key)) {
			@Override
			public ScanResult<Tuple> execute(Jedis j) {
				return j.zscan(key, cursor);
			}
		}.run();
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
		return new JedisHandler<ScanResult<Entry<String, String>>>(this.getShard(key)) {
			@Override
			public ScanResult<Entry<String, String>> execute(Jedis j) {
				return j.hscan(key, cursor);
			}
		}.run();
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
		return new JedisHandler<ScanResult<Entry<String, String>>>(this.getShard(key)) {
			@Override
			public ScanResult<Entry<String, String>> execute(Jedis j) {
				return j.hscan(key, cursor, params);
			}
		}.run();
	}

	@Override
	public ScanResult<String> sscan(String key, String cursor) {
		return new JedisHandler<ScanResult<String>>(this.getShard(key)) {
			@Override
			public ScanResult<String> execute(Jedis j) {
				return j.sscan(key, cursor);
			}
		}.run();
	}

	@Override
	public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
		return new JedisHandler<ScanResult<String>>(this.getShard(key)) {
			@Override
			public ScanResult<String> execute(Jedis j) {
				return j.sscan(key, cursor, params);
			}
		}.run();
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor) {
		return new JedisHandler<ScanResult<Tuple>>(this.getShard(key)) {
			@Override
			public ScanResult<Tuple> execute(Jedis j) {
				return j.zscan(key, cursor);
			}
		}.run();
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
		return new JedisHandler<ScanResult<Tuple>>(this.getShard(key)) {
			@Override
			public ScanResult<Tuple> execute(Jedis j) {
				return j.zscan(key, cursor, params);
			}
		}.run();
	}

	@Override
	public Long pfadd(String key, String... elements) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.pfadd(key, elements);
			}
			
		}.run();
	}

	@Override
	public long pfcount(String key) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.pfadd(key);
			}
			
		}.run();
	}

	@Override
	public Long geoadd(String key, double longitude, double latitude, String member) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.geoadd(key, longitude, latitude, member);
			}
			
		}.run();
	}

	@Override
	public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.geoadd(key, memberCoordinateMap);
			}
			
		}.run();
	}

	@Override
	public Double geodist(String key, String member1, String member2) {
		return new JedisHandler<Double>(this.getShard(key)){

			@Override
			public Double execute(Jedis jedis) {
				return jedis.geodist(key, member1, member2);
			}
			
		}.run();
	}

	@Override
	public Double geodist(String key, String member1, String member2, GeoUnit unit) {
		return new JedisHandler<Double>(this.getShard(key)){

			@Override
			public Double execute(Jedis jedis) {
				return jedis.geodist(key, member1, member2, unit);
			}
			
		}.run();
	}

	@Override
	public List<String> geohash(String key, String... members) {
		return new JedisHandler<List<String>>(this.getShard(key)){

			@Override
			public List<String> execute(Jedis jedis) {
				return jedis.geohash(key, members);
			}
			
		}.run();
	}

	@Override
	public List<GeoCoordinate> geopos(String key, String... members) {
		return new JedisHandler<List<GeoCoordinate>>(this.getShard(key)){

			@Override
			public List<GeoCoordinate> execute(Jedis jedis) {
				return jedis.geopos(key, members);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius,
			GeoUnit unit) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadius(key, longitude, latitude, radius, unit);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadius(key, longitude, latitude, radius, unit, param);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadiusByMember(key, member, radius, unit);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadiusByMember(key, member, radius, unit, param);
			}
			
		}.run();
	}

}
