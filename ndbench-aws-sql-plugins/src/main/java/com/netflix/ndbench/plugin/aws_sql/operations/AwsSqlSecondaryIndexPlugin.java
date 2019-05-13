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
@NdBenchClientPlugin("AwsSqlSecondaryIndexPlugin")
public class AwsSqlSecondaryIndexPlugin extends AwsSqlPluginBase
{
    private static final Logger logger = LoggerFactory.getLogger(AwsSqlSecondaryIndexPlugin.class);

    private static String readFromMainQuery = "SELECT pkey, %s FROM %s where pkey = ";
    private static String writeToMainQuery = "INSERT IGNORE INTO %s (pkey, %s) VALUES ";

    @Inject
    public AwsSqlSecondaryIndexPlugin(AwsSqlConfiguration configuration)
    {
        super(configuration);
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        try (Connection connection = readOnlyPool.getConnection())
        {
            ResultSet rs = connection.createStatement().executeQuery(readFromMainQuery + "'" + key + "'");
            int rsSize = 0;
            while (rs.next())
            {
                rsSize++;
            }

            if (rsSize == 0)
            {
                return CacheMiss;
            }

            if (rsSize > 1)
            {
                throw new Exception("Expecting only 1 row with a given key: " + key);
            }

            return ResultOK;
        }
    }

    @Override
    public String writeSingle(String key) throws Exception
    {
        try (Connection connection = readWritePool.getConnection())
        {
            String columns = getNDelimitedStrings(config.getColsPerRow());
            connection.createStatement()
                      .executeUpdate(writeToMainQuery + "('" + key + "', " + columns + ")");
            return ResultOK;
        }
    }

    public void createTables() throws Exception
    {
        try (Connection connection = readWritePool.getConnection())
        {
            String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i + " varchar(100)").collect(Collectors.joining(", "));
            connection.createStatement()
                      .execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pkey varchar(100), %s, PRIMARY KEY(pkey))",
                                             config.getDBName(), config.getTableName(), columns));

            // mysql doesn't have a non-shitty (non-race-y) way to check and then create an index
            // create secondary indices
            try
            {
                for (int i = 0; i < config.getColsPerRow(); i++)
                {
                    connection.createStatement()
                              .execute(String.format("CREATE INDEX IF NOT EXISTS %s_column%d_index on %s (column%d)",
                                                     config.getTableName(), i, config.getTableName(), i));
                }
            }
            catch (Exception e)
            {
                logger.warn("problem creating indices; they probably already exist - you have been warned");
            }
        }
    }

    public void prepareStatements()
    {
        String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i).collect(Collectors.joining(", "));
        readFromMainQuery = String.format(readFromMainQuery, columns, config.getTableName());
        writeToMainQuery = String.format(writeToMainQuery, config.getTableName(), columns);
    }
}
