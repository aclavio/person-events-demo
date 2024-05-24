package io.confluent.demo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.confluent.demo.util.DeathSourceReferenceUtil;
import io.confluent.demo.util.ZipcodeReferenceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ReferenceDataTest {

    ObjectMapper mapper = new JsonMapper();
    JedisPool pool = new JedisPool("localhost", 6379);

    class ZipcodeKey {
        @JsonProperty("zip_code")
        public String zipCode;
    }

    @Test
    public void testManualGetZipcode() {
        try (Jedis jedis = pool.getResource()) {
            Assertions.assertEquals("OK", jedis.select(1));
            Assertions.assertNotNull(jedis.get("{\"zip_code\":\"33556\"}"));

            ZipcodeKey key = new ZipcodeKey();
            key.zipCode = "33556";

            String resp = jedis.get(mapper.writeValueAsString(key));
            Assertions.assertNotNull(resp);
            JsonNode val = mapper.readTree(resp);
            Assertions.assertEquals("FL", val.get("official_usps_state_code").asText());
        } catch (JacksonException e) {
            Assertions.fail("Jackson exception");
        } catch (Exception e) {
            Assertions.fail("jedis exception");
        }
    }

    @Test
    public void testGetZipcode() {
        try {
            ZipcodeReferenceUtil zipcodeUtil = new ZipcodeReferenceUtil(pool, mapper);
            JsonNode resp = zipcodeUtil.getZipcodeReference("33556");
            Assertions.assertNotNull(resp);
            Assertions.assertEquals("FL", resp.get("official_usps_state_code").asText());
            Assertions.assertEquals("Florida", resp.get("official_state_name").asText());
            Assertions.assertEquals("Odessa", resp.get("official_usps_city_name").asText());
            Assertions.assertEquals("America/New_York", resp.get("timezone").asText());
        } catch (Exception e) {
            Assertions.fail("ZipcodeReferenceUtil exception");
        }
    }

    @Test
    public void testGetDeathSource() {
        try {
            DeathSourceReferenceUtil dsUtil = new DeathSourceReferenceUtil(pool, mapper);
            JsonNode resp = dsUtil.getDeathSourceReference("VA");
            Assertions.assertNotNull(resp);
            Assertions.assertEquals("Veteran Affairs", resp.get("death_source").asText());
        } catch (Exception e) {
            Assertions.fail("DeathSourceReferenceUtil exception");
        }
    }

}
