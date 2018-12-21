package com.netflix.ndbench.plugin.faunadb.operations;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faunadb.client.errors.BadRequestException;
import com.faunadb.client.errors.NotFoundException;
import com.faunadb.client.errors.UnknownException;
import com.faunadb.client.query.Expr;
import com.faunadb.client.types.Result;
import com.faunadb.client.types.Value;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.faunadb.config.FaunaDbConfiguration;

import static com.faunadb.client.query.Language.Arr;
import static com.faunadb.client.query.Language.Class;
import static com.faunadb.client.query.Language.Create;
import static com.faunadb.client.query.Language.CreateClass;
import static com.faunadb.client.query.Language.CreateIndex;
import static com.faunadb.client.query.Language.Database;
import static com.faunadb.client.query.Language.Delete;
import static com.faunadb.client.query.Language.Do;
import static com.faunadb.client.query.Language.Exists;
import static com.faunadb.client.query.Language.Get;
import static com.faunadb.client.query.Language.If;
import static com.faunadb.client.query.Language.Index;
import static com.faunadb.client.query.Language.Match;
import static com.faunadb.client.query.Language.Obj;
import static com.faunadb.client.query.Language.Value;

/**
 * Create a medium-size doc, with some nested fields, along with some indices.
 * Doc represents a simple/naive user schema.
 * doc (a/k/a class) sample:
 * {
 *     "name": string,
 *     "birth_timestamp": int,
 *     "address:
 *     {
 *         "street": string,
 *         "city: string,
 *     },
 *     "pets": [string],
 *     contacts:
 *     {
 *         emails: [string],
 *         phone_numbers:
 *         {
 *             mobile: [int],
 *             home: [int]
 *         },
 *     },
 * }
 */
@Singleton
@NdBenchClientPlugin("FaunaDbLargeDocWithIndicesPlugin")
public class FaunaDbLargeDocWithIndicesPlugin extends FaunaDbPluginBase
{
    private static final Logger logger = LoggerFactory.getLogger(FaunaDbLargeDocWithIndicesPlugin.class);
    private static final String NAME_INDEX = "users_by_name";
    private static final String CITY_INDEX = "users_by_city";
    private static final String EMAIL_INDEX = "users_by_email";
    private static final String MOBILE_INDEX = "users_by_mobile_phone";


    @Inject
    public FaunaDbLargeDocWithIndicesPlugin(FaunaDbConfiguration configuration) {
        super(configuration);
    }

    public void createTables() {
        Expr expr = CreateClass(Obj("name", Value(className)));
        Value retVal = createConditionally(expr, className, false);
        logger.info("CREATE CLASS response: {}", retVal);

        // create 2Is here, if you need ... which you most definitely do need.

        // index for name
        expr = CreateIndex(Obj("name", Value(NAME_INDEX),   // this is the name of the index
                               "source", Class(Value(className)), // class type
                               "terms", Arr(Obj("field", Arr(Value("data"), Value("name"))))
//                               "unique", Value(true) // lol, i *think* this is how to set a uniqueness constraint (facepalm)
        ));
        retVal = createConditionally(expr, NAME_INDEX, true);
        logger.info("CREATE INDEX response: {}", retVal);

        // index for city
        expr = CreateIndex(Obj("name", Value(CITY_INDEX),   // this is the name of the index
                                "source", Class(Value(className)), // class type
                                "terms", Arr(Obj("field", Arr(Value("data"), Value("address"), Value("city"))))
        ));
        retVal = createConditionally(expr, CITY_INDEX, true);
        logger.info("CREATE INDEX response: {}", retVal);

        // index for email
        expr = CreateIndex(Obj("name", Value(EMAIL_INDEX),   // this is the name of the index
                               "source", Class(Value(className)), // class type
                               "terms", Arr(Obj("field", Arr(Value("data"), Value("contacts"), Value("emails"))))
//                               "unique", Value(true) // lol, i *think* this is how to set a uniqueness constraint (facepalm)
        ));
        retVal = createConditionally(expr, EMAIL_INDEX, true);
        logger.info("CREATE INDEX response: {}", retVal);

        // index for mobile phone number
        expr = CreateIndex(Obj("name", Value(MOBILE_INDEX),   // this is the name of the index
                               "source", Class(Value(className)), // class type
                               "terms", Arr(Obj("field", Arr(Value("data"), Value("contacts"), Value("phone_numbers"), Value("mobile"))))
//                               "unique", Value(true) // lol, i *think* this is how to set a uniqueness constraint (facepalm)
        ));
        retVal = createConditionally(expr, MOBILE_INDEX, true);
        logger.info("CREATE INDEX response: {}", retVal);
    }

    private Value createConditionally(Expr expr, String instanceName, boolean isIndex) {
        logger.info("NEXT CREATE CONDITIONALLY: {}", expr);

        try {
            Expr targetInstance = isIndex ? Index(instanceName) : Class(instanceName);


            // TODO:JEB this works when bringing up a fresh cluster, the "delete previous if existing" if far from working,
            // but at least it leaves the existing class/indices in place. whatever, fauna ....
            // Thus, you need to delete the faunadb files before each ndbench run
            return client.query(Do(
                                    If(
                                        Exists(targetInstance),
                                        Delete(targetInstance),
                                        Value(true)
                                    ),
                                    expr
            )).get();
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause == null)
                throw new RuntimeException(ee);
            if (cause instanceof BadRequestException) {
                // assume the database already exists, and no-op
                logger.info("BadRequestException response: {}", ee);
            } else if (cause instanceof UnknownException) {
                logger.error("couldn't create a class or index: {}", ee);
                throw (UnknownException)cause;
            } else {
                throw new RuntimeException("unknown error", ee);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public String writeSingle(String key) throws Exception {
        // build up values for doc
        String name = dataGenerator.getRandomValue();
        int birth = Math.abs(dataGenerator.getRandomInteger());
        String street = dataGenerator.getRandomValue();
        String city = dataGenerator.getRandomString();

        String[] pets = generateStrings(null);
        String[] emails = generateStrings("@example.com");
        int[] mobileNumbers = generateInts();
        int[] homeNumbers = generateInts();


        Value retVal = client.query(Create(Class(Value(className)), Obj("data",
                                                                                 Obj("name", Value(name),
                                                                                     "birth_timestamp", Value(birth),
                                                                                     "address", Obj("street", Value(street), "city", Value(city)),
                                                                                     "pets", Value(pets),
                                                                                     "contacts", Obj("emails", Value(emails),
                                                                                                     "phone_numbers", Obj("mobile", Value(mobileNumbers),
                                                                                                                          "home", Value(homeNumbers)))
        )))).get();
        return ResultOK;
    }

    private String[] generateStrings(String suffix) {
        int count = Math.abs(dataGenerator.getRandomInteger()) & 0x7;
        String[] strings = new String[count];
        for (int i = 0; i < count; i++) {
            strings[i] = dataGenerator.getRandomString();
            if (suffix != null)
                strings[i] = strings[i] + suffix;
        }
        return strings;
    }

    private int[] generateInts() {
        int count = Math.abs(dataGenerator.getRandomInteger()) & 0x7;
        int[] ints = new int[count];
        for (int i = 0; i < count; i++)
            ints[i] = dataGenerator.getRandomInteger();
        return ints;
    }

    public String readSingle(String key) throws Exception {
        String indexName;
        int idx = Math.abs(dataGenerator.getRandomInteger()) & 0x3;
        switch (idx) {
            case 0: indexName = NAME_INDEX; break;
            case 1: indexName = CITY_INDEX; break;
            case 2: indexName = EMAIL_INDEX; break;
            case 3: indexName = MOBILE_INDEX; break;
            default:    throw new IllegalArgumentException("bad index: " + idx);
        }

        Value retVal;
        try {
            retVal = client.query(Get(Match(Index(Value(indexName)), Value(key)))).get();
        } catch (ExecutionException ee) {
            if (!(ee.getCause() instanceof NotFoundException))
                throw ee;
            return ResultOK;
        }
        logger.info("READ doc: {}", retVal);

        Result<String> result = retVal.to(String.class);
        return result.isSuccess() ? ResultOK : ResultFailed;
    }
}
