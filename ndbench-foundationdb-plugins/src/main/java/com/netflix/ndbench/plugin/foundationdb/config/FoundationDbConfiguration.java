package com.netflix.ndbench.plugin.foundationdb.config;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Configuration(prefix = NdBenchConstants.PROP_NAMESPACE + "foundationdb")
public interface FoundationDbConfiguration {
    @DefaultValue("perftest")
    String getDBName();

    @DefaultValue("test")
    String getTableName();

    @DefaultValue("test-loadbalancer")
    String getLoadBalancer();

    @DefaultValue("26257")
    String getPort();

    @DefaultValue("")
    String getPassword();

    @DefaultValue("5")
    Integer getColsPerRow();

    @DefaultValue("100")
    String getPoolSize();
}