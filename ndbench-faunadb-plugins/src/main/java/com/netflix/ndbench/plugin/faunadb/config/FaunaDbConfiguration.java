package com.netflix.ndbench.plugin.faunadb.config;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

@Configuration(prefix = NdBenchConstants.PROP_NAMESPACE + "cockroachdb")
public interface FaunaDbConfiguration {
    @DefaultValue("perftest")
    String getDBName();

    @DefaultValue("test")
    String getClassName();

    @PropertyName(name = "host")
    // Ignore PMD java rule as inapplicable because we're setting an overridable default
    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    @DefaultValue("127.0.0.1")
    String getHost();

    @PropertyName(name = "host.port")
    @DefaultValue("8443")
    Integer getHostPort();


    @PropertyName(name = "root_key")
    @DefaultValue("secret")
    String getRookKeySecret();

    @DefaultValue("")
    String getPassword();

    @DefaultValue("5")
    Integer getColsPerRow();

    @DefaultValue("100")
    String getPoolSize();
}

