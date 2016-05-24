package com.yuwnloy.shardedsplitpooljedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public abstract class JedisHandler<T> {
	private JedisPool pool;

	public JedisHandler(JedisPool jedisPool) {
		this.pool = jedisPool;
	}

	public abstract T execute(Jedis jedis);

	public T run() {
		Jedis jedis = pool.getResource();

		try {
			return this.execute(jedis);
		} finally {
			if (jedis.getClient().isConnected()) {
				this.pool.returnResource(jedis);
			} else {
				this.pool.returnBrokenResource(jedis);
			}
		}

	}
}
