package com.netflix.ndbench.plugin.faunadb.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faunadb.client.FaunaClient;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.faunadb.config.FaunaDbConfiguration;


/**
 * basis use of the fauna client HOWTO:
 * https://app.fauna.com/documentation/howto/crud
 *
 * But, how to more thoroughly use the driver:
 * https://github.com/fauna/faunadb-jvm/blob/master/docs/java.md
 */
public abstract class FaunaDbPluginBase implements NdBenchClient
{
    private static final Logger logger = LoggerFactory.getLogger(FaunaDbPluginBase.class);
    protected static final String ResultOK = "Ok";
    protected static final String ResultFailed = "Failed";

//    private static final DynamicStringProperty faunadbLoadBalancer = DynamicPropertyFactory.getInstance()
//                                                                                        .getStringProperty(NdBenchConstants.PROP_NAMESPACE + "faunadb.loadbalancer", "cdecrdbnossl--useast-elb-1849424046.us-east-1.elb.amazonaws.com");

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

    public void init(DataGenerator dataGenerator) throws Exception
    {
        this.dataGenerator = dataGenerator;

        client = FaunaClient.builder()
                            .withEndpoint("http://" + config.getHost() + ':' + config.getHostPort())
                            .build();

        createTables();
    }

    public abstract void createTables() throws Exception;

    public void shutdown()
    {
        client.close();
    }

    public String getConnectionInfo() throws Exception
    {
        return String.format("Connected to faunadb: %s using driver: %s as user :%s");
    }

    public String runWorkFlow()
    {
        return null;
    }
}