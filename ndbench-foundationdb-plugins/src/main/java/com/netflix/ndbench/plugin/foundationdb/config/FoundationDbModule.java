package com.netflix.ndbench.plugin.foundationdb.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPluginGuiceModule;

@NdBenchClientPluginGuiceModule
public class FoundationDbModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    FoundationDbConfiguration getFoundationDbConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FoundationDbConfiguration.class);
    }
}
