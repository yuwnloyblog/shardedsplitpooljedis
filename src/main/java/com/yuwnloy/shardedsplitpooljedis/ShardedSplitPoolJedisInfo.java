package com.yuwnloy.shardedsplitpooljedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.util.ShardInfo;
import redis.clients.util.Sharded;

public class ShardedSplitPoolJedisInfo extends ShardInfo<JedisPool> {
	public String toString() {
		return host + ":" + port + "*" + getWeight();
	}

	private int timeout;
	private String host;
	private int port;
	private String password = null;
	private String name = null;
	private int timeoutForLog = 500;
	private GenericObjectPoolConfig poolConfig;

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public ShardedSplitPoolJedisInfo(final GenericObjectPoolConfig poolConfig, String host) {
		this(poolConfig, host, Protocol.DEFAULT_PORT);
	}

	public ShardedSplitPoolJedisInfo(final GenericObjectPoolConfig poolConfig, String host, String name) {
		this(poolConfig, host, Protocol.DEFAULT_PORT, name);
	}

	public ShardedSplitPoolJedisInfo(final GenericObjectPoolConfig poolConfig, String host, int port) {
		this(poolConfig, host, port, 2000);
	}

	public ShardedSplitPoolJedisInfo(final GenericObjectPoolConfig poolConfig, String host, int port, String name) {
		this(poolConfig, host, port, 2000, name);
	}

	public ShardedSplitPoolJedisInfo(final GenericObjectPoolConfig poolConfig, String host, int port, int timeout) {
		this(poolConfig, host, port, timeout, Sharded.DEFAULT_WEIGHT);
	}

	public ShardedSplitPoolJedisInfo(final GenericObjectPoolConfig poolConfig, String host, int port, int timeout, String name) {
		this(poolConfig, host, port, timeout, Sharded.DEFAULT_WEIGHT);
		this.name = name;
	}

	public ShardedSplitPoolJedisInfo(final GenericObjectPoolConfig poolConfig, String host, int port, int timeout, int weight) {
		super(weight);
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.poolConfig = poolConfig;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String auth) {
		this.password = auth;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public JedisPool createResource() {
		return new JedisPool(this.poolConfig,this.host,this.port,this.timeout);
	}

	public int getTimeoutForLog() {
		return timeoutForLog;
	}

	public void setTimeoutForLog(int timeoutForLog) {
		this.timeoutForLog = timeoutForLog;
	}
}
