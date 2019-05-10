/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.ndbench.plugin.aws_sql.operations;

import java.sql.Connection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.aws_sql.configs.AwsSqlConfiguration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public abstract class AwsSqlPluginBase implements NdBenchClient
{
    protected static final String ResultOK = "Ok";
    protected static final String ResultFailed = "Failed";
    protected static final String CacheMiss = null;
    protected static final String ResultAmbiguous = "Failed";
    private static final Logger logger = LoggerFactory.getLogger(AwsSqlPluginBase.class);

    protected DataGenerator dataGenerator;

    protected final AwsSqlConfiguration config;

    protected static HikariDataSource readWritePool;
    protected static HikariDataSource readOnlyPool;

    protected AwsSqlPluginBase(AwsSqlConfiguration awsSqlConfiguration) {
        this.config = awsSqlConfiguration;
    }

    @Override
    public void init(DataGenerator dataGenerator) throws Exception
    {
        this.dataGenerator = dataGenerator;
        logger.info("Initializing the AwsSql client");


        try
        {
            readWritePool = new HikariDataSource(buildHikariConfig(config.getReadWriteUrl()));
            readOnlyPool = new HikariDataSource(buildHikariConfig(config.getReadOnlyUrls()));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Exception during connection initialization", e);
        }

        logger.info("Connected to an AWS sql database, initializing/creating the table(s)");
        createTables();
        logger.info("Created tables");
        prepareStatements();
    }

    private HikariConfig buildHikariConfig(String urls)
    {
        // These settings are from HikariCP's great config page for MySQL,
        // https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
        HikariConfig hikariConfig = new HikariConfig();
//        hikariConfig.addDataSourceProperty("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        String jdbcurl = String.format("jdbc:mysql://%s/%s", urls, this.config.getDBName());
        hikariConfig.setJdbcUrl(jdbcurl);
        hikariConfig.setUsername(config.getUser());
        hikariConfig.setPassword(config.getPassword());


        hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", "2048");
        hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.useLocalSessionState", "true");
        hikariConfig.addDataSourceProperty("dataSource.rewriteBatchedStatements", "true");
        hikariConfig.addDataSourceProperty("dataSource.cacheResultSetMetadata", "true");
        hikariConfig.addDataSourceProperty("dataSource.cacheServerConfiguration", "true");
        hikariConfig.addDataSourceProperty("dataSource.elideSetAutoCommits", "true");
        hikariConfig.addDataSourceProperty("dataSource.maintainTimeStats", "false");

        return hikariConfig;
    }

    /**
     * Shutdown the client
     */
    @Override
    public void shutdown()
    {
        readWritePool.close();
        readOnlyPool.close();
    }

    /**
     * Get connection info
     */
    @Override
    public String getConnectionInfo() throws Exception
    {
        Connection connection = readWritePool.getConnection();

        String info =  String.format("Connected to database: %s using driver: %s as user :%s",
                             connection.getMetaData().getDatabaseProductName(),
                             connection.getMetaData().getDriverName(),
                             connection.getMetaData().getUserName());

        connection.close();

        return info;
    }

    @Override
    public String runWorkFlow()
    {
        return null;
    }

    public abstract void createTables() throws Exception;

    public abstract void prepareStatements();

    /**
     * Assumes delimiter to be comma since that covers all the usecase for now.
     * Will parameterize if use cases differ on delimiter.
     * @param n
     * @return
     */
    public String getNDelimitedStrings(int n)
    {
        return IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "'" + dataGenerator.getRandomValue() + "'").collect(Collectors.joining(","));
    }
}
