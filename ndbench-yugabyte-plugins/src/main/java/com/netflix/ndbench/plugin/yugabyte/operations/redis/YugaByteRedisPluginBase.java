package com.netflix.ndbench.plugin.yugabyte.operations.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.yugabyte.configs.YugaByteConfiguration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public abstract class YugaByteRedisPluginBase implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(YugaByteRedisPluginBase.class);

    private final YugaByteConfiguration config;

    protected DataGenerator dataGenerator;
    protected JedisPool clientPool;

    protected YugaByteRedisPluginBase(YugaByteConfiguration config) {
        this.config = config;
    }

    @Override
    public void init(DataGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;

        // Note: you totally need to use the JediaPool in a multi-threaded app,
        // see https://github.com/xetorthio/jedis/wiki/Getting-started#using-jedis-in-a-multithreaded-environment
        clientPool = new JedisPool(new JedisPoolConfig(), config.getHost(), config.getRedisPort());
    }

    /**
     * Shutdown the client
     */
    @Override
    public void shutdown() {
        clientPool.close();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() {
        return "connected to YugaByte-Redis at " + config.getHost() + ':' + config.getRedisPort();
    }

    @Override
    public String runWorkFlow()
    {
        return null;
    }
}
