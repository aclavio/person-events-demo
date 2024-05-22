package io.confluent.demo.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ZipcodeReferenceUtil {

    private static final Logger logger = LoggerFactory.getLogger(ZipcodeReferenceUtil.class);

    private final JedisPool jedisPool;
    private final ObjectMapper mapper;

    public ZipcodeReferenceUtil(JedisPool jedisPool, ObjectMapper mapper) {
        this.jedisPool = jedisPool;
        this.mapper = mapper;
    }

    public JsonNode getZipcodeReference(String zipcode) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(1);
            String refdata = jedis.get(mapper.writeValueAsString(new ZipcodeKey(zipcode)));
            JsonNode zipdata = mapper.readTree(refdata);
            logger.debug(zipdata.toPrettyString());
            return zipdata;
        } catch (Exception e) {
            logger.error("Error getting reference data", e);
            throw new RuntimeException("Error getting reference data");
        }
    }

    static class ZipcodeKey {
        @JsonProperty("zip_code")
        public String zipCode;

        public ZipcodeKey(String zip) {
            this.zipCode = zip;
        }
    }

}
