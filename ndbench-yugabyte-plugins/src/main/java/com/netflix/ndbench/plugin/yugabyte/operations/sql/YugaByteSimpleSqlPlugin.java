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

package com.netflix.ndbench.plugin.yugabyte.operations.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.yugabyte.configs.YugaByteConfiguration;

@Singleton
@NdBenchClientPlugin("YugaByteSimpleSqlPlugin")
public class YugaByteSimpleSqlPlugin extends YugaByteSqlPluginBase {
    private static final Logger logger = LoggerFactory.getLogger(YugaByteSimpleSqlPlugin.class);

    private static String readQuery = "SELECT key, column1, %s FROM %s where key = ";
    private static String writeQuery = "UPSERT INTO %s (key, column1, %s) VALUES ";

    @Inject
    public YugaByteSimpleSqlPlugin(YugaByteConfiguration YugaByteConfiguration) {
        super(YugaByteConfiguration);
    }

    public void createTables() throws Exception {
        String values = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "value" + i + " STRING").collect(Collectors.joining(", "));

        Connection connection = ds.getConnection();

        String createTable = String.format("CREATE TABLE IF NOT EXISTS %s.%s (key STRING PRIMARY KEY, column1 INT, %s)",
                                           config.getPostgresDBName(), config.getTableName(), values);
        logger.info("Yugabyte create table statement: {}", createTable);
        connection.createStatement().execute(createTable);

        connection.close();
    }

    public void prepareStatements() {
        String values = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "value" + i).collect(Collectors.joining(", "));
        readQuery = String.format(readQuery, values, config.getTableName());
        writeQuery = String.format(writeQuery, config.getTableName(), values);
    }

    @Override
    public String readSingle(String key) throws Exception {
        Connection connection = ds.getConnection();
        ResultSet rs = connection.createStatement().executeQuery(readQuery + "'" + key + "'");

        int rsSize = 0;
        while (rs.next()) {
            rsSize++;
        }

        if (rsSize == 0) {
            connection.close();
            return CacheMiss;
        }

        if (rsSize > 1) {
            connection.close();
            throw new Exception("Expecting only 1 row with a given key: " + key);
        }

        connection.close();
        return ResultOK;
    }

    @Override
    public String writeSingle(String key) throws Exception {
        String values = getNDelimitedStrings(config.getColsPerRow());
        Connection connection = ds.getConnection();

        connection.createStatement().executeUpdate(writeQuery + "('" + key + "', 1 ," + values + ")");
        connection.close();

        return ResultOK;
    }
}
