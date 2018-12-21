package com.netflix.ndbench.plugin.faunadb.operations;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faunadb.client.errors.BadRequestException;
import com.faunadb.client.errors.UnknownException;
import com.faunadb.client.types.Result;
import com.faunadb.client.types.Value;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.faunadb.config.FaunaDbConfiguration;

import static com.faunadb.client.query.Language.*;

/**
 * Creates a trivial doc with just one element, at the root.
 */
@Singleton
@NdBenchClientPlugin("FaunaDbSimplePlugin")
public class FaunaDbSimplePlugin extends FaunaDbPluginBase {
    private static final Logger logger = LoggerFactory.getLogger(FaunaDbSimplePlugin.class);

    @Inject
    public FaunaDbSimplePlugin(FaunaDbConfiguration configuration) {
        super(configuration);
    }

    public void createTables() throws Exception {
        // create the class (it's like a type or schema for the instances in this database).
        // need the try-catch block to handle when the class and/or index already exist.
        try {
            Value retVal = client.query(CreateClass(Obj("name", Value(className)))).get();

            // create 2I here, if you need ... which you most definitely do need
            retVal = client.query(CreateIndex(Obj(
            "name", Value("posts_by_title"),   // this is the name of the index
            "source", Class(Value(className)), // class type
            "terms", Arr(Obj("field", Arr(Value("data"), Value("title")))) // indexes title field
            ))).get();
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause == null)
                throw ee;
            if (cause instanceof BadRequestException) {
                // assume the database already exists, and no-op
            } else if (cause instanceof UnknownException) {
                logger.error("couldn't create a class or index");
                throw (UnknownException)cause;
            } else {
                throw new RuntimeException("unknown error", cause);
            }
        }
    }

    public String writeSingle(String key) throws Exception {
        Value retVal = client.query(Create(Class(Value(className)), Obj("data", Obj("title", Value(key))))).get();
        return ResultOK;
    }

    public String readSingle(String key) throws Exception {
        // fauna does best when you use the instance's reference value (basically a surrogate primary key).
        // this example comes from the HOWTO docs
        //client.query(Get(Ref(Class("posts"),Value(192903209792046592L)))).get();

        // however, lacking the reference value, the only sane way of finding information is by using a pre-baked index
        // (need to supply the index name here, as we don't have well-defined schema)
        Value retVal = client.query(Get(Match(Index(Value("posts_by_title")), Value(key)))).get();

        // i think this is the only way to check for success/failure of the query is to coerce it into an object,
        // which is wrapped by the Result<T>, and ask the Result for the status
        Result<String> result = retVal.to(String.class);
        return result.isSuccess() ? ResultOK : ResultFailed;
    }
}
