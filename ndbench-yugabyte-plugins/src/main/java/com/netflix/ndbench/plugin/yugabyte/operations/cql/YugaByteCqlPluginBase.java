package com.netflix.ndbench.plugin.yugabyte.operations.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.yugabyte.configs.YugaByteConfiguration;

public abstract class YugaByteCqlPluginBase implements NdBenchClient {
    private final YugaByteConfiguration config;

    protected DataGenerator dataGenerator;
    protected Cluster cluster;
    protected Session session;

    protected YugaByteCqlPluginBase(YugaByteConfiguration config) {
        this.config = config;
    }

    @Override
    public void init(DataGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;

        // not sure how to set the port, so assume 9042? (cql native transport port)
        cluster = Cluster.builder()
                                 .addContactPoint(config.getHost())
                                 .build();
        session = cluster.connect();

        createTables();
    }

    protected abstract void createTables();

    /**
     * Shutdown the client
     */
    @Override
    public void shutdown() {
        session.close();
        cluster.close();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() {
        return "connected to YugaByte-CQL at " + config.getHost() + ':' + config.getRedisPort();
    }

    @Override
    public String runWorkFlow()
    {
        return null;
    }


}
