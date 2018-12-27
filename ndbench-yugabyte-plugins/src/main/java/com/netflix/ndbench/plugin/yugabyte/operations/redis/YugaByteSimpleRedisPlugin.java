package com.netflix.ndbench.plugin.yugabyte.operations.redis;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.plugin.yugabyte.configs.YugaByteConfiguration;
import redis.clients.jedis.Jedis;

@Singleton
@NdBenchClientPlugin("YugaByteSimpleRedisPlugin")
public class YugaByteSimpleRedisPlugin extends YugaByteRedisPluginBase {
    private static final Logger logger = LoggerFactory.getLogger(YugaByteSimpleRedisPlugin.class);

    @Inject
    public YugaByteSimpleRedisPlugin(YugaByteConfiguration configuration) {
        super(configuration);
    }

    @Override
    public String writeSingle(String key) {
        Map<String, String> userProfile = new HashMap<>();
        userProfile.put("name", dataGenerator.getRandomValue());
        userProfile.put("favorite_number", dataGenerator.getRandomInteger().toString());
        userProfile.put("language", dataGenerator.getRandomString());

        try (Jedis jedis = clientPool.getResource()) {
            String result = jedis.hmset(key, userProfile);
        } catch (Exception e) {
            logger.info("failed to WRITE", e);
        }
        return "OK";
    }

    @Override
    public String readSingle(String key) {
        try (Jedis jedis = clientPool.getResource()) {
            Map<String, String> userData = jedis.hgetAll(key);
        } catch (Exception e) {
            logger.info("failed to READ", e);
        }
        return "OK";
    }
}
