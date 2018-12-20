package com.netflix.ndbench.plugin.faunadb.operations;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faunadb.client.FaunaClient;
import com.faunadb.client.errors.BadRequestException;
import com.faunadb.client.errors.UnknownException;
import com.faunadb.client.query.Language;
import com.faunadb.client.types.Value;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.faunadb.config.FaunaDbConfiguration;

import static com.faunadb.client.query.Language.CreateDatabase;
import static com.faunadb.client.query.Language.CreateKey;
import static com.faunadb.client.query.Language.Database;
import static com.faunadb.client.query.Language.Obj;


/**
 * basis use of the fauna client HOWTO:
 * https://app.fauna.com/documentation/howto/crud
 *
 * But, how to more thoroughly use the driver:
 * https://github.com/fauna/faunadb-jvm/blob/master/docs/java.md
 */
public abstract class FaunaDbPluginBase implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(FaunaDbPluginBase.class);
    protected static final String ResultOK = "Ok";
    protected static final String ResultFailed = "Failed";

    protected final FaunaDbConfiguration config;
    protected final String dbName;
    protected final String className;

    protected FaunaClient client;
    protected DataGenerator dataGenerator;

    protected FaunaDbPluginBase(FaunaDbConfiguration config) {
        this.config = config;
        dbName = config.getDBName();

        // this might not be table name
        className = config.getTableName();
    }

    public void init(DataGenerator dataGenerator) throws Exception {
        this.dataGenerator = dataGenerator;

        client = FaunaClient.builder()
                            .withSecret(config.getRookKeySecret())
                            .withEndpoint(endpoint())
                            .build();

        // create the 'database', loosely equivalent to a keyspace in c*
        // need the try-catch block to handle the case when the database already exists.
        Value retVal = null;
        try {
            retVal = client.query(CreateDatabase(Obj("name", Language.Value(dbName)))).get();
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause == null)
                throw ee;
            if (cause instanceof BadRequestException) {
                // assume the database already exists, and no-op
            } else if (cause instanceof UnknownException) {
                logger.error("couldn't create a faunadb database - it's very likely you haven't run 'fauna init' to initialize the cluster");
                throw (UnknownException)cause;
            } else {
                throw new RuntimeException("unknown error", cause);
            }
        }

        // Create an initial server key by using an admin key. The server key has unrestricted access to a single database;
        // in this case, the server key will only access the database we just created
        retVal = client.query(CreateKey(Obj("database", Database(Language.Value(dbName)), "role", Language.Value("server")))).get();

        createTables();
    }

    private String endpoint() {
        return "http://" + config.getHost() + ':' + config.getHostPort();
    }


    public abstract void createTables() throws Exception;

    public void shutdown() {
        client.close();
    }

    public String getConnectionInfo() {
        return String.format("Connected to faunadb at address %s", endpoint());
    }

    public String runWorkFlow() {
        return null;
    }
}