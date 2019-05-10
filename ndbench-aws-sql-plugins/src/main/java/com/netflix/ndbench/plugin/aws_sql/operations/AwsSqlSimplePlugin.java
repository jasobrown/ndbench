/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.ndbench.plugin.aws_sql.operations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.aws_sql.configs.AwsSqlConfiguration;

@Singleton
@NdBenchClientPlugin("AwsSqlSimplePlugin")
public class AwsSqlSimplePlugin extends AwsSqlPluginBase
{
    private static final Logger logger = LoggerFactory.getLogger(AwsSqlSimplePlugin.class);

    private static String readQuery = "SELECT perf_key, column1, %s FROM %s where perf_key = ";
    private static String writeQuery = "INSERT IGNORE INTO %s (perf_key, column1, %s) VALUES ";

    @Inject
    public AwsSqlSimplePlugin(AwsSqlConfiguration cockroachDBConfiguration) {
        super(cockroachDBConfiguration);
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        try (Connection connection = readOnlyPool.getConnection())
        {
            ResultSet rs = connection.createStatement().executeQuery(readQuery + "'" + key + "'");
            int rsSize = 0;
            while (rs.next())
                rsSize++;

            if (rsSize == 0)
                return CacheMiss;

            if (rsSize > 1)
                throw new Exception("Expecting only 1 row with a given key: " + key);

            return ResultOK;
        }
    }

    @Override
    public String writeSingle(String key) throws Exception
    {
        try (Connection connection = readWritePool.getConnection())
        {
            String values = getNDelimitedStrings(config.getColsPerRow());

            connection.createStatement()
                      .executeUpdate(writeQuery + "('" + key + "', 1 ," + values + ")");
            return ResultOK;
        }
    }

    public void createTables() throws Exception
    {
        try (Connection connection = readWritePool.getConnection())
        {
            String values = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "value" + i + " varchar(100)").collect(Collectors.joining(", "));
            String create = String.format("CREATE TABLE IF NOT EXISTS %s.%s (perf_key varchar(100), column1 INT, %s, PRIMARY KEY(perf_key))",
                                          config.getDBName(), config.getTableName(), values);
            logger.info("creating a new table: {}", create);
            connection.createStatement().execute(create);
        }
    }

    public void prepareStatements()
    {
        //NOTE: not *actually* preparing statements (as per 'standard' client-database convention),
        // but eagerly constructing some string for query use
        String values = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "value" + i).collect(Collectors.joining(", "));
        readQuery = String.format(readQuery, values, config.getTableName());
        writeQuery = String.format(writeQuery, config.getTableName(), values);
    }
}
