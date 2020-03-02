package org.lili.redis.inaction.chapter4;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/2/29 23:43
 * @description
 * @notes
 */

public class RedisManagerTest {


    private RedisManager redisManager;

    @Before
    public void setup() {
        redisManager = new RedisManager();
    }

    @Test
    public void info() {
        redisManager.info();
    }

    @Test
    public void configAll() {
        redisManager.configAll();
    }
}