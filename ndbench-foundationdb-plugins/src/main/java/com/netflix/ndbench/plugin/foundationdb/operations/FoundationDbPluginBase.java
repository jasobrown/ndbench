package com.netflix.ndbench.plugin.foundationdb.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.plugin.foundationdb.config.FoundationDbConfiguration;

public abstract class FoundationDbPluginBase implements NdBenchClient {
    private static final Logger logger = LoggerFactory.getLogger(FoundationDbPluginBase.class);

    /**
     * This is supposed to represent the 'semver' for 6.0.0. Pray we never have to do
     * a point version based on existing release (for example, we need to patch 6.0.0, we have
     * to call it 6.0.0.1).
     */
    private static final int FDB_API_VERSION = 600;

    protected static final String ResultOK = "Ok";
    protected static final String ResultFailed = "Failed";
    protected static final String ResultAmbiguous = "Ambiguous";

    protected DataGenerator dataGenerator;
    protected final FoundationDbConfiguration config;

    protected FDB fdbClient;
    protected Database fdbDatabase;

    protected FoundationDbPluginBase(FoundationDbConfiguration config) {
        this.config = config;
    }

    @Override
    public void init(DataGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;

        // create client instance
        fdbClient = FDB.selectAPIVersion(FDB_API_VERSION);

        // it's impossible to write out an on-demand cluster-file to the tmp filesystem,
        // and pass that to the FDB.open(file) call, instead of using the default.
        // this is because the server-side creates and updates the cluster-file when
        // coordination memberships occur. Thus, we're kinda screwed with automated,
        // cloud-based testing as you need to "copy over the server cluster-file
        // to the client machines" (facepalm)

        // this opens a connection and a database using the default cluster file.
        // Note: always need a reference to a Database, to issue (transactional) updates/queries.
        // there is no real concept of keyspaces/tables in FDB - they offer layers for Tuples,
        // which help users create subspaces and directories. These concepts are supported in the
        // client-binding libraries.
        fdbDatabase = fdbClient.open();
    }

    @Override
    public String runWorkFlow() {
        return null;
    }

    @Override
    public String getConnectionInfo() {
        return "This is a connection to a FoundationDB cluster. " +
               "This client app has no idea what happens at the native library level :/";
    }

    @Override
    public void shutdown() {
        fdbDatabase.close();
    }
}
