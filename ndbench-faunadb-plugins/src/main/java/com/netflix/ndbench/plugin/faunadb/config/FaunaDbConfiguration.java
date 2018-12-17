package com.netflix.ndbench.plugin.faunadb.config;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Configuration(prefix = NdBenchConstants.PROP_NAMESPACE + "cockroachdb")
public interface FaunaDbConfiguration {
    @DefaultValue("perftest")
    String getDBName();

    @DefaultValue("test")
    String getTableName();

    @DefaultValue("test-loadbalancer")
    String getLoadBalancer();

    @DefaultValue("maxroach")
    String getUser();

    @DefaultValue("26257")
    String getPort();

    @DefaultValue("")
    String getPassword();

    @DefaultValue("5")
    Integer getColsPerRow();

    @DefaultValue("100")
    String getPoolSize();
}

