package com.netflix.ndbench.plugin.aws_sql.operations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.aws_sql.configs.AwsSqlConfiguration;

@Singleton
@NdBenchClientPlugin("AwsSqlBasicTransactionPlugin")
public class AwsSqlBasicTransactionPlugin extends AwsSqlPluginBase
{
    private static final Logger logger = LoggerFactory.getLogger(AwsSqlBasicTransactionPlugin.class);

    private static String readFromMainQuery = "SELECT pkey, %s FROM %s where pkey = ";
    private static String writeToMainQuery = "INSERT IGNORE INTO %s (pkey, %s) VALUES ";
    private static String writeToChildQuery = "INSERT IGNORE INTO child%d (pkey, column1, value) VALUES ";

    @Inject
    public AwsSqlBasicTransactionPlugin(AwsSqlConfiguration awsSqlConfiguration)
    {
        super(awsSqlConfiguration);
    }

    @Override
    public String writeSingle(String key) throws Exception
    {
        try (Connection connection = readWritePool.getConnection())
        {
            //execute transaction
            String[] childKeys = new String[config.getColsPerRow()];
            for (int i = 0; i < config.getColsPerRow(); i++)
            {
                childKeys[i] = "'" + dataGenerator.getRandomValue() + "'";
            }

            connection.setAutoCommit(false);

            Connection closurePtr = connection;

            RetryableTransaction transaction = conn -> {
                Statement statement = closurePtr.createStatement();

                // write to main table
                String main = writeToMainQuery + "('" + key + "', " + StringUtils.join(childKeys, ',') + ")";
//                logger.info("\tmain table = {}", main);
                statement.addBatch(main);

                // writes to child tables
                for (int i = 0; i < config.getColsPerRow(); i++)
                {
                    String subtable = String.format(writeToChildQuery, i) + "(" + childKeys[i] + ", 1, '" + dataGenerator.getRandomValue() + "')";
//                    logger.info("\t\tsubtable = {}", subtable);
                    statement.addBatch(subtable);
                }

//                logger.info("*** end batch ***");
                statement.executeBatch();
            };

            Savepoint sp = connection.setSavepoint("tx_restart");

            while (true)
            {
                boolean releaseAttempted = false;
                try
                {
                    transaction.run(connection);
                    releaseAttempted = true;
                    connection.releaseSavepoint(sp);
                    break;
                }
                catch (SQLException e)
                {
                    String sqlState = e.getSQLState();

                    // Check if the error code indicates a SERIALIZATION_FAILURE.
                    // Note: this check worked under crdb, no idea if it's useful for mysql
                    if (sqlState.equals("40001"))
                    {
                        // Signal the database that we will attempt a retry.
                        connection.rollback(sp);
                    }
                    else if (releaseAttempted)
                    {
                        throw e;
                    }
                    else
                    {
                        throw e;
                    }
                }
            }

            connection.commit();
            connection.setAutoCommit(true);

            return ResultOK;
        }
    }

    @Override
    public String readSingle(String key) throws Exception
    {
        try (Connection connection = readOnlyPool.getConnection())
        {
            ResultSet rs = connection.createStatement().executeQuery(readFromMainQuery + "'" + key + "'");
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
    public void createTables() throws Exception
    {
        try (Connection connection = readWritePool.getConnection())
        {
            String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i + " varchar(100)").collect(Collectors.joining(", "));
            connection.createStatement()
                      .execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pkey varchar(100), %s, PRIMARY KEY (pkey))",
                                             config.getDBName(), config.getTableName(), columns));

            // create child tables
            for (int i = 0; i < config.getColsPerRow(); i++)
            {
                connection.createStatement()
                          .execute(String.format("CREATE TABLE IF NOT EXISTS %s.child%d (pkey varchar(100), column1 INT, value varchar(100), PRIMARY KEY(pkey))",
                                                 config.getDBName(), i));
            }
        }
    }

    @Override
    public void prepareStatements()
    {
        String columns = IntStream.range(0, config.getColsPerRow()).mapToObj(i -> "column" + i).collect(Collectors.joining(", "));
        readFromMainQuery = String.format(readFromMainQuery, columns, config.getTableName());
        writeToMainQuery = String.format(writeToMainQuery, config.getTableName(), columns);
    }
}
