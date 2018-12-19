package com.netflix.ndbench.plugin.foundationdb.operations;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.foundationdb.config.FoundationDbConfiguration;

@Singleton
@NdBenchClientPlugin("FoundationDbSimplePlugin")
public class FoundationDbSimplePlugin extends FoundationDbPluginBase {

    @Inject
    public FoundationDbSimplePlugin(FoundationDbConfiguration config) {
        super(config);
    }

    public String readSingle(String key) throws Exception {
        return fdbDatabase.run(tx -> {
            try {
                byte[] val = tx.get(Tuple.from(key).pack()).join();
                String retVal = Tuple.fromBytes(val).getString(0);
                return ResultOK;
            } catch (FDBException e) {
                // why the &^%! is there a success field on an exception?!?!
                if (e.isSuccess()) {
                    return ResultOK;
                } else {
                    return ResultFailed;
                }
            }
        });
    }

    public String writeSingle(String key) {
        return fdbDatabase.run(tx -> {
            try {
                tx.set(Tuple.from(key).pack(), Tuple.from(dataGenerator.getRandomString()).pack());
                return ResultOK;
            } catch (FDBException e) {
                // why the &^%! is there a success field on an exception?!?!
                if (e.isSuccess()) {
                    return ResultOK;
                } else if (e.isMaybeCommitted() || e.isRetryableNotCommitted()) {
                    return ResultAmbiguous;
                } else {
                    return ResultFailed;
                }
            }
        });
    }
}
