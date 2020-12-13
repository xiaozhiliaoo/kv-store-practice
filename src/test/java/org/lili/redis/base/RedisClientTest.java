package org.lili.redis.base;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/2/13 1:28
 * @description
 * @notes
 */

public class RedisClientTest {

    @Test
    public void incr() {
        for (int i = 0; i < 20; i++) {
            RedisClient.newClient().incr("fast");
        }
    }

    @Test
    public void dump() {
        byte[] dumps = RedisClient.newClient().dump("myhash");
        System.out.println(new String(dumps));
    }
}