package org.lili.redis.base;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/2/13 1:28
 * @description
 * @notes
 */

public class RedisSlowClientTest {

    @Test
    public void incr() {
        for (int i = 0; i < 20; i++) {
            RedisSlowClient.newClient().incr("slow");
        }
    }
}