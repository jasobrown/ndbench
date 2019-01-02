package com.netflix.ndbench.plugin.yugabyte.operations.cql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.yugabyte.configs.YugaByteConfiguration;

@Singleton
@NdBenchClientPlugin("YugaByteSimpleCqlPlugin")
public class YugaByteSimpleCqlPlugin extends YugaByteCqlPluginBase
{
    private static final Logger logger = LoggerFactory.getLogger(YugaByteSimpleCqlPlugin.class);

    private final String keyspace;
    private final String table;

    private PreparedStatement insertStatement;
    private PreparedStatement selectStatement;

    @Inject
    public YugaByteSimpleCqlPlugin(YugaByteConfiguration configuration)
    {
        super(configuration);
        keyspace = configuration.getKeyspaceName();
        table = configuration.getTableName();
    }

    protected void createTables() {
        String createkeyspace = String.format("CREATE keyspace IF NOT EXISTS %s;", keyspace);
        ResultSet createkeyspaceResult = session.execute(createkeyspace);

        String createtable = String.format("CREATE table IF NOT EXISTS %s.%s (id varchar PRIMARY KEY, " +
                                           "name varchar, age int, language varchar);",
                                           keyspace, table);
        ResultSet createResult = session.execute(createtable);

        String insert = String.format("insert into %s.%s (id, name, age, language) values (?, ?, ?, ?);", keyspace, table);
        insertStatement = session.prepare(insert);

        String select = String.format("select name, age, language from %s.%s where id = ?;", keyspace, table);
        selectStatement = session.prepare(select);
    }

    @Override
    public String writeSingle(String key) {
        String name = dataGenerator.getRandomString();
        Integer age = dataGenerator.getRandomIntegerValue();
        String lang = dataGenerator.getRandomValue();
        BoundStatement bound = insertStatement.bind(key, name, age, lang);
        session.execute(bound);
        return "OK";
    }

    @Override
    public String readSingle(String key) {
        session.execute(selectStatement.bind(key));
        return "OK";
    }
}
