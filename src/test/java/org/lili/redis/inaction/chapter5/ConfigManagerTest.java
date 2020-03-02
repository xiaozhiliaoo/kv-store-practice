package org.lili.redis.inaction.chapter5;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 16:55
 * @description
 * @notes
 */

public class ConfigManagerTest {

    private ConfigManager configManager;

    @Before
    public void before() {
        configManager = new ConfigManager();
    }

    @Test
    public void redisConnection() {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("db", 15);
        configManager.setConfig("redis", "test", config);

        Jedis conn2 = configManager.redisConnection("test");
        System.out.println(
                "We can run commands from the configured connection: " + (conn2.info() != null));
    }
}