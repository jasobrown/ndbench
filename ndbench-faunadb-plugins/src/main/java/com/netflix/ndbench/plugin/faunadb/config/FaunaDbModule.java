package com.netflix.ndbench.plugin.faunadb.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;

public class FaunaDbModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    FaunaDbConfiguration getCockroachDBConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FaunaDbConfiguration.class);
    }
}
