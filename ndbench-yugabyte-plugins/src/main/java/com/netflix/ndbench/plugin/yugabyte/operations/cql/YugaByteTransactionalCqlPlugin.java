package com.netflix.ndbench.plugin.yugabyte.operations.cql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.inject.Inject;
import com.netflix.ndbench.plugin.yugabyte.configs.YugaByteConfiguration;

/**
 * Ideas borrowed from YB's docs: https://docs.yugabyte.com/latest/develop/learn/acid-transactions/
 */
public class YugaByteTransactionalCqlPlugin extends YugaByteCqlPluginBase
{
    private static final Logger logger = LoggerFactory.getLogger(YugaByteTransactionalCqlPlugin.class);

    private static final String KEYSPACE = "yb_ndbenchdemo";
    private static final String TABLE = "employees_transactional";

    private PreparedStatement insertStatement;
    private PreparedStatement selectStatement;

    @Inject
    public YugaByteTransactionalCqlPlugin(YugaByteConfiguration configuration)
    {
        super(configuration);
    }

    protected void createTables() {
        String createKeyspace = String.format("CREATE KEYSPACE IF NOT EXISTS %s;", KEYSPACE);
        ResultSet createKeyspaceResult = session.execute(createKeyspace);

        String createTable = String.format("CREATE TABLE IF NOT EXISTS %s.%s (id varchar PRIMARY KEY, " +
                                           "name varchar, age int, language varchar) " +
                                           "with transactions = { 'enabled' : true };",
                                           KEYSPACE, TABLE);
        ResultSet createResult = session.execute(createTable);

        // TODO:JEB complete me!!!

//        String insert = String.format("insert into %s.%s (id, name, age, language) values (?, ?, ?, ?);", KEYSPACE, TABLE);
//        insertStatement = session.prepare(insert);
//
//        String select = String.format("select name, age, language from %s.%s where id = ?;", KEYSPACE, TABLE);
//        selectStatement = session.prepare(select);
    }

    @Override
    public String writeSingle(String key) {
//        String name = dataGenerator.getRandomString();
//        Integer age = dataGenerator.getRandomIntegerValue();
//        String lang = dataGenerator.getRandomValue();
//        BoundStatement bound = insertStatement.bind(key, name, age, lang);
//        session.execute(bound);
        return "OK";
    }

    @Override
    public String readSingle(String key) {
//        session.execute(selectStatement.bind(key));
        return "OK";
    }
}
