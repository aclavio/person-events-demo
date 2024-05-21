package io.confluent.demo.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class DeathSourceReferenceUtil {

    private static final Logger logger = LoggerFactory.getLogger(DeathSourceReferenceUtil.class);

    private final JedisPool jedisPool;
    private final ObjectMapper mapper;

    public DeathSourceReferenceUtil(JedisPool jedisPool, ObjectMapper mapper) {
        this.jedisPool = jedisPool;
        this.mapper = mapper;
    }

    public JsonNode getDeathSourceReference(String code) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(1);
            String refdata = jedis.get(mapper.writeValueAsString(new DeathSourceKey(code)));
            JsonNode dsRef = mapper.readTree(refdata);
            logger.info(dsRef.toPrettyString());
            return dsRef;
        } catch (Exception e) {
            logger.error("Error getting reference data", e);
            throw new RuntimeException("Error getting reference data");
        }
    }

    static class DeathSourceKey {
        @JsonProperty("code")
        public String code;

        public DeathSourceKey(String code) {
            this.code = code;
        }
    }

}
