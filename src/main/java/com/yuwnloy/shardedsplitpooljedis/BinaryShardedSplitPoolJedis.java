package com.yuwnloy.shardedsplitpooljedis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

public class BinaryShardedSplitPoolJedis extends Sharded<JedisPool, ShardedSplitPoolJedisInfo> implements BinaryJedisCommands {

	public BinaryShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards) {
		super(shards);
	}

	public BinaryShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards, Hashing algo) {
		super(shards, algo);
	}

	public BinaryShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards, Pattern keyTagPattern) {
		super(shards, keyTagPattern);
	}

	public BinaryShardedSplitPoolJedis(List<ShardedSplitPoolJedisInfo> shards, Hashing algo, Pattern keyTagPattern) {
		super(shards, algo, keyTagPattern);
	}

	public void destroy() {
		for (JedisPool jPool : getAllShards()) {
			jPool.destroy();
		}
	}

	@Override
	public String set(final byte[] key, final byte[] value) {

		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.set(key, value);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] get(final byte[] key) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.get(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Boolean exists(final byte[] key) {

		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				Boolean ret = j.exists(key);
				return ret;
			}
		}.run();

	}

	@Override
	public String type(final byte[] key) {

		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.type(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long expire(final byte[] key, final int seconds) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.expire(key, seconds);
				return ret;
			}
		}.run();

	}

	@Override
	public Long expireAt(final byte[] key, final long unixTime) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.expireAt(key, unixTime);
				return ret;
			}
		}.run();

	}

	@Override
	public Long ttl(final byte[] key) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.ttl(key);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] getSet(final byte[] key, final byte[] value) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.getSet(key, value);
				return ret;
			}
		}.run();

	}

	@Override
	public Long setnx(final byte[] key, final byte[] value) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.setnx(key, value);
				return ret;
			}
		}.run();

	}

	@Override
	public String setex(final byte[] key, final int seconds, final byte[] value) {

		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.setex(key, seconds, value);
				return ret;
			}
		}.run();

	}

	@Override
	public Long decrBy(final byte[] key, final long integer) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.decrBy(key, integer);
				return ret;
			}
		}.run();

	}

	@Override
	public Long decr(final byte[] key) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.decr(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long incrBy(final byte[] key, final long integer) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.incrBy(key, integer);
				return ret;
			}
		}.run();

	}

	@Override
	public Long incr(final byte[] key) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.incr(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long append(final byte[] key, final byte[] value) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.append(key, value);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] substr(final byte[] key, final int start, final int end) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.substr(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Long hset(final byte[] key, final byte[] field, final byte[] value) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hset(key, field, value);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] hget(final byte[] key, final byte[] field) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.hget(key, field);
				return ret;
			}
		}.run();

	}

	@Override
	public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hsetnx(key, field, value);
				return ret;
			}
		}.run();

	}

	@Override
	public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {

		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.hmset(key, hash);
				return ret;
			}
		}.run();

	}

	@Override
	public List<byte[]> hmget(final byte[] key, final byte[]... fields) {

		return new JedisHandler<List<byte[]>>(this.getShard(key)) {
			@Override
			public List<byte[]> execute(Jedis j) {
				List<byte[]> ret = j.hmget(key, fields);
				return ret;
			}
		}.run();

	}

	@Override
	public Long hincrBy(final byte[] key, final byte[] field, final long value) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hincrBy(key, field, value);
				return ret;
			}
		}.run();

	}

	@Override
	public Boolean hexists(final byte[] key, final byte[] field) {

		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				Boolean ret = j.hexists(key, field);
				return ret;
			}
		}.run();

	}

	@Override
	public Long hdel(final byte[] key, final byte[]... field) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hdel(key, field);
				return ret;
			}
		}.run();

	}

	@Override
	public Long hlen(final byte[] key) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.hlen(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> hkeys(final byte[] key) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.hkeys(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Collection<byte[]> hvals(final byte[] key) {

		return new JedisHandler<Collection<byte[]>>(this.getShard(key)) {
			@Override
			public Collection<byte[]> execute(Jedis j) {
				Collection<byte[]> ret = j.hvals(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Map<byte[], byte[]> hgetAll(final byte[] key) {

		return new JedisHandler<Map<byte[], byte[]>>(this.getShard(key)) {
			@Override
			public Map<byte[], byte[]> execute(Jedis j) {
				Map<byte[], byte[]> ret = j.hgetAll(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long rpush(final byte[] key, final byte[]... string) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.rpush(key, string);
				return ret;
			}
		}.run();

	}

	@Override
	public Long lpush(final byte[] key, final byte[]... string) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.lpush(key, string);
				return ret;
			}
		}.run();

	}

	@Override
	public Long llen(final byte[] key) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.llen(key);
				return ret;
			}
		}.run();

	}

	@Override
	public List<byte[]> lrange(final byte[] key, final long start, final long end) {

		return new JedisHandler<List<byte[]>>(this.getShard(key)) {
			@Override
			public List<byte[]> execute(Jedis j) {
				List<byte[]> ret = j.lrange(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public String ltrim(final byte[] key, final long start, final long end) {

		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.ltrim(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] lindex(final byte[] key, final long index) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.lindex(key, index);
				return ret;
			}
		}.run();

	}

	@Override
	public String lset(final byte[] key, final long index, final byte[] value) {

		return new JedisHandler<String>(this.getShard(key)) {
			@Override
			public String execute(Jedis j) {
				String ret = j.lset(key, index, value);
				return ret;
			}
		}.run();

	}

	@Override
	public Long lrem(final byte[] key, final long count, final byte[] value) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.lrem(key, count, value);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] lpop(final byte[] key) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.lpop(key);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] rpop(final byte[] key) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.rpop(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long sadd(final byte[] key, final byte[]... member) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.sadd(key, member);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> smembers(final byte[] key) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.smembers(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long srem(final byte[] key, final byte[]... member) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.srem(key, member);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] spop(final byte[] key) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.spop(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long scard(final byte[] key) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.scard(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Boolean sismember(final byte[] key, final byte[] member) {

		return new JedisHandler<Boolean>(this.getShard(key)) {
			@Override
			public Boolean execute(Jedis j) {
				Boolean ret = j.sismember(key, member);
				return ret;
			}
		}.run();

	}

	@Override
	public byte[] srandmember(final byte[] key) {

		return new JedisHandler<byte[]>(this.getShard(key)) {
			@Override
			public byte[] execute(Jedis j) {
				byte[] ret = j.srandmember(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zadd(final byte[] key, final double score, final byte[] member) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zadd(key, score, member);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> zrange(final byte[] key, final long start, final long end) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrange(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zrem(final byte[] key, final byte[]... member) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zrem(key, member);
				return ret;
			}
		}.run();

	}

	@Override
	public Double zincrby(final byte[] key, final double score, final byte[] member) {

		return new JedisHandler<Double>(this.getShard(key)) {
			@Override
			public Double execute(Jedis j) {
				Double ret = j.zincrby(key, score, member);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zrank(final byte[] key, final byte[] member) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zrank(key, member);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zrevrank(final byte[] key, final byte[] member) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zrevrank(key, member);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrevrange(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long end) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeWithScores(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrevrangeWithScores(final byte[] key, final long start, final long end) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeWithScores(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zcard(final byte[] key) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zcard(key);
				return ret;
			}
		}.run();

	}

	@Override
	public Double zscore(final byte[] key, final byte[] member) {

		return new JedisHandler<Double>(this.getShard(key)) {
			@Override
			public Double execute(Jedis j) {
				Double ret = j.zscore(key, member);
				return ret;
			}
		}.run();

	}

	@Override
	public List<byte[]> sort(final byte[] key) {

		return new JedisHandler<List<byte[]>>(this.getShard(key)) {
			@Override
			public List<byte[]> execute(Jedis j) {
				List<byte[]> ret = j.sort(key);
				return ret;
			}
		}.run();

	}

	@Override
	public List<byte[]> sort(final byte[] key, final SortingParams sortingParameters) {

		return new JedisHandler<List<byte[]>>(this.getShard(key)) {
			@Override
			public List<byte[]> execute(Jedis j) {
				List<byte[]> ret = j.sort(key, sortingParameters);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zcount(final byte[] key, final double min, final double max) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zcount(key, min, max);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zcount(final byte[] key, final byte[] min, final byte[] max) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zcount(key, min, max);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrangeByScore(key, min, max);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
			final int count) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrangeByScore(key, min, max, offset, count);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset,
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
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrangeByScoreWithScores(key, min, max);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset,
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
	public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrevrangeByScore(key, max, min);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset,
			final int count) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrevrangeByScore(key, max, min, offset, count);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrevrangeByScore(key, max, min);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset,
			final int count) {

		return new JedisHandler<Set<byte[]>>(this.getShard(key)) {
			@Override
			public Set<byte[]> execute(Jedis j) {
				Set<byte[]> ret = j.zrevrangeByScore(key, max, min, offset, count);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeByScoreWithScores(key, max, min);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min, final int offset,
			final int count) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeByScoreWithScores(key, max, min, offset, count);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeByScoreWithScores(key, max, min);
				return ret;
			}
		}.run();

	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min, final int offset,
			final int count) {

		return new JedisHandler<Set<Tuple>>(this.getShard(key)) {
			@Override
			public Set<Tuple> execute(Jedis j) {
				Set<Tuple> ret = j.zrevrangeByScoreWithScores(key, max, min, offset, count);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zremrangeByRank(final byte[] key, final long start, final long end) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zremrangeByRank(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zremrangeByScore(final byte[] key, final double start, final double end) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zremrangeByScore(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.zremrangeByScore(key, start, end);
				return ret;
			}
		}.run();

	}

	@Override
	public Long linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot, final byte[] value) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.linsert(key, where, pivot, value);
				return ret;
			}
		}.run();

	}
//
//	@Override
//	public Long objectRefcount(final byte[] key) {
//
//		return new JedisHandler<Long>(this.getShard(key)) {
//			@Override
//			public Long execute(Jedis j) {
//				Long ret = j.objectRefcount(key);
//				return ret;
//			}
//		}.run();
//
//	}

//	@Override
//	public Long objectIdletime(final byte[] key) {
//
//		return new JedisHandler<Long>(this.getShard(key)) {
//			@Override
//			public Long execute(Jedis j) {
//				Long ret = j.objectIdletime(key);
//				return ret;
//			}
//		}.run();
//
//	}

//	@Override
//	public byte[] objectEncoding(final byte[] key) {
//
//		return new JedisHandler<byte[]>(this.getShard(key)) {
//			@Override
//			public byte[] execute(Jedis j) {
//				byte[] ret = j.objectEncoding(key);
//				return ret;
//			}
//		}.run();
//
//	}

	@Override
	public Long lpushx(final byte[] key, final byte[]... string) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.lpushx(key, string);
				return ret;
			}
		}.run();

	}

	@Override
	public Long rpushx(final byte[] key, final byte[]... string) {

		return new JedisHandler<Long>(this.getShard(key)) {
			@Override
			public Long execute(Jedis j) {
				Long ret = j.rpushx(key, string);
				return ret;
			}
		}.run();

	}

	public ShardedSplitPoolJedisPipeline pipelined() {
		ShardedSplitPoolJedisPipeline pipeline = new ShardedSplitPoolJedisPipeline();
		pipeline.setShardedPoolJedis(this);
		return pipeline;
	}

	@Override
	public String set(byte[] key, byte[] value, byte[] nxxx) {
		return new JedisHandler<String>(this.getShard(key)){

			@Override
			public String execute(Jedis jedis) {
				return jedis.set(key, value, nxxx);
			}
			
		}.run();
	}

	@Override
	public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
		return new JedisHandler<String>(this.getShard(key)){

			@Override
			public String execute(Jedis jedis) {
				return jedis.set(key, value, nxxx, expx, time);
			}
			
		}.run();
	}

	@Override
	public Long persist(byte[] key) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.persist(key);
			}
			
		}.run();
	}

	@Override
	public Long pexpire(String key, long milliseconds) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.pexpire(key, milliseconds);
			}
			
		}.run();
	}

	@Override
	public Long pexpire(byte[] key, long milliseconds) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.pexpire(key, milliseconds);
			}
			
		}.run();
	}

	@Override
	public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.pexpireAt(key, millisecondsTimestamp);
			}
			
		}.run();
	}

	@Override
	public Boolean setbit(byte[] key, long offset, boolean value) {
		return new JedisHandler<Boolean>(this.getShard(key)){

			@Override
			public Boolean execute(Jedis jedis) {
				return jedis.setbit(key, offset, value);
			}
			
		}.run();
	}

	@Override
	public Boolean setbit(byte[] key, long offset, byte[] value) {
		return new JedisHandler<Boolean>(this.getShard(key)){

			@Override
			public Boolean execute(Jedis jedis) {
				return jedis.setbit(key, offset, value);
			}
			
		}.run();
	}

	@Override
	public Boolean getbit(byte[] key, long offset) {
		return new JedisHandler<Boolean>(this.getShard(key)){

			@Override
			public Boolean execute(Jedis jedis) {
				return jedis.getbit(key, offset);
			}
			
		}.run();
	}

	@Override
	public Long setrange(byte[] key, long offset, byte[] value) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.setrange(key, offset, value);
			}
			
		}.run();
	}

	@Override
	public byte[] getrange(byte[] key, long startOffset, long endOffset) {
		return new JedisHandler<byte[]>(this.getShard(key)){

			@Override
			public byte[] execute(Jedis jedis) {
				return jedis.getrange(key, startOffset, endOffset);
			}
			
		}.run();
	}

	@Override
	public Double incrByFloat(byte[] key, double value) {
		return new JedisHandler<Double>(this.getShard(key)){

			@Override
			public Double execute(Jedis jedis) {
				return jedis.incrByFloat(key, value);
			}
			
		}.run();
	}

	@Override
	public Double hincrByFloat(byte[] key, byte[] field, double value) {
		return new JedisHandler<Double>(this.getShard(key)){

			@Override
			public Double execute(Jedis jedis) {
				return jedis.hincrByFloat(key, field, value);
			}
			
		}.run();
	}

	@Override
	public Set<byte[]> spop(byte[] key, long count) {
		return new JedisHandler<Set<byte[]>>(this.getShard(key)){

			@Override
			public Set<byte[]> execute(Jedis jedis) {
				return jedis.spop(key, count);
			}
			
		}.run();
	}

	@Override
	public List<byte[]> srandmember(byte[] key, int count) {
		return new JedisHandler<List<byte[]>>(this.getShard(key)){

			@Override
			public List<byte[]> execute(Jedis jedis) {
				return jedis.srandmember(key, count);
			}
			
		}.run();
	}

	@Override
	public Long strlen(byte[] key) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.strlen(key);
			}
			
		}.run();
	}

	@Override
	public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.zadd(key, score, member, params);
			}
			
		}.run();
	}

	@Override
	public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.zadd(key, scoreMembers);
			}
			
		}.run();
	}

	@Override
	public Long zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.zadd(key, scoreMembers, params);
			}
			
		}.run();
	}

	@Override
	public Double zincrby(byte[] key, double score, byte[] member, ZIncrByParams params) {
		return new JedisHandler<Double>(this.getShard(key)){

			@Override
			public Double execute(Jedis jedis) {
				return jedis.zincrby(key, score, member, params);
			}
			
		}.run();
	}

	@Override
	public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
		return new JedisHandler<Set<byte[]>>(this.getShard(key)){

			@Override
			public Set<byte[]> execute(Jedis jedis) {
				return jedis.zrangeByScore(key, min, max);
			}
			
		}.run();
	}

	@Override
	public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
		return new JedisHandler<Set<byte[]>>(this.getShard(key)){

			@Override
			public Set<byte[]> execute(Jedis jedis) {
				return jedis.zrangeByScore(key, min, max,offset,count);
			}
			
		}.run();
	}

	@Override
	public Long zlexcount(byte[] key, byte[] min, byte[] max) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.zlexcount(key, min, max);
			}
			
		}.run();
	}

	@Override
	public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
		return new JedisHandler<Set<byte[]>>(this.getShard(key)){

			@Override
			public Set<byte[]> execute(Jedis jedis) {
				return jedis.zrangeByLex(key, min, max);
			}
			
		}.run();
	}

	@Override
	public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
		return new JedisHandler<Set<byte[]>>(this.getShard(key)){

			@Override
			public Set<byte[]> execute(Jedis jedis) {
				return jedis.zrangeByLex(key, min, max, offset, count);
			}
			
		}.run();
	}

	@Override
	public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
		return new JedisHandler<Set<byte[]>>(this.getShard(key)){

			@Override
			public Set<byte[]> execute(Jedis jedis) {
				return jedis.zrevrangeByLex(key, max, min);
			}
			
		}.run();
	}

	@Override
	public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
		return new JedisHandler<Set<byte[]>>(this.getShard(key)){

			@Override
			public Set<byte[]> execute(Jedis jedis) {
				return jedis.zrevrangeByLex(key, max, min, offset, count);
			}
			
		}.run();
	}

	@Override
	public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.zremrangeByLex(key, min, max);
			}
			
		}.run();
	}

	@Override
	public List<byte[]> blpop(byte[] arg) {
		return new JedisHandler<List<byte[]>>(this.getShard(arg)){

			@Override
			public List<byte[]> execute(Jedis jedis) {
				return jedis.blpop(arg);
			}
			
		}.run();
	}

	@Override
	public List<byte[]> brpop(byte[] arg) {
		return new JedisHandler<List<byte[]>>(this.getShard(arg)){

			@Override
			public List<byte[]> execute(Jedis jedis) {
				return jedis.brpop(arg);
			}
			
		}.run();
	}

	@Override
	public Long del(byte[] key) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.del(key);
			}
			
		}.run();
	}

	@Override
	public byte[] echo(byte[] arg) {
		return new JedisHandler<byte[]>(this.getShard(arg)){

			@Override
			public byte[] execute(Jedis jedis) {
				return jedis.echo(arg);
			}
			
		}.run();
	}

	@Override
	public Long move(byte[] key, int dbIndex) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.move(key, dbIndex);
			}
			
		}.run();
	}

	@Override
	public Long bitcount(byte[] key) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.bitcount(key);
			}
			
		}.run();
	}

	@Override
	public Long bitcount(byte[] key, long start, long end) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.bitcount(key, start, end);
			}
			
		}.run();
	}

	@Override
	public Long pfadd(byte[] key, byte[]... elements) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.pfadd(key, elements);
			}
			
		}.run();
	}

	@Override
	public long pfcount(byte[] key) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.pfcount(key);
			}
			
		}.run();
	}

	@Override
	public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.geoadd(key, longitude, latitude, member);
			}
			
		}.run();
	}

	@Override
	public Long geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
		return new JedisHandler<Long>(this.getShard(key)){

			@Override
			public Long execute(Jedis jedis) {
				return jedis.geoadd(key, memberCoordinateMap);
			}
			
		}.run();
	}

	@Override
	public Double geodist(byte[] key, byte[] member1, byte[] member2) {
		return new JedisHandler<Double>(this.getShard(key)){

			@Override
			public Double execute(Jedis jedis) {
				return jedis.geodist(key, member1, member2);
			}
			
		}.run();
	}

	@Override
	public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
		return new JedisHandler<Double>(this.getShard(key)){

			@Override
			public Double execute(Jedis jedis) {
				return jedis.geodist(key, member1, member2, unit);
			}
			
		}.run();
	}

	@Override
	public List<byte[]> geohash(byte[] key, byte[]... members) {
		return new JedisHandler<List<byte[]>>(this.getShard(key)){

			@Override
			public List<byte[]> execute(Jedis jedis) {
				return jedis.geohash(key, members);
			}
			
		}.run();
	}

	@Override
	public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
		return new JedisHandler<List<GeoCoordinate>>(this.getShard(key)){

			@Override
			public List<GeoCoordinate> execute(Jedis jedis) {
				return jedis.geopos(key, members);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius,
			GeoUnit unit) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadius(key, longitude, latitude, radius, unit);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadius(key, longitude, latitude, radius, unit, param);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadiusByMember(key, member, radius, unit);
			}
			
		}.run();
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		return new JedisHandler<List<GeoRadiusResponse>>(this.getShard(key)){

			@Override
			public List<GeoRadiusResponse> execute(Jedis jedis) {
				return jedis.georadiusByMember(key, member, radius, unit, param);
			}
			
		}.run();
	}

	@Override
	public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
		return new JedisHandler<ScanResult<Entry<byte[],byte[]>>>(this.getShard(key)){

			@Override
			public ScanResult<Entry<byte[], byte[]>> execute(Jedis jedis) {
				return jedis.hscan(key, cursor);
			}
			
		}.run();
	}

	@Override
	public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
		return new JedisHandler<ScanResult<Entry<byte[],byte[]>>>(this.getShard(key)){

			@Override
			public ScanResult<Entry<byte[], byte[]>> execute(Jedis jedis) {
				return jedis.hscan(key, cursor, params);
			}
			
		}.run();
	}

	@Override
	public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
		return new JedisHandler<ScanResult<byte[]>>(this.getShard(key)){

			@Override
			public ScanResult<byte[]> execute(Jedis jedis) {
				return jedis.sscan(key, cursor);
			}
			
		}.run();
	}

	@Override
	public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
		return new JedisHandler<ScanResult<byte[]>>(this.getShard(key)){

			@Override
			public ScanResult<byte[]> execute(Jedis jedis) {
				return jedis.sscan(key, cursor, params);
			}
			
		}.run();
	}

	@Override
	public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
		return new JedisHandler<ScanResult<Tuple>>(this.getShard(key)){

			@Override
			public ScanResult<Tuple> execute(Jedis jedis) {
				return jedis.zscan(key, cursor);
			}
			
		}.run();
	}

	@Override
	public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
		return new JedisHandler<ScanResult<Tuple>>(this.getShard(key)){

			@Override
			public ScanResult<Tuple> execute(Jedis jedis) {
				return jedis.zscan(key, cursor, params);
			}
			
		}.run();
	}
}
