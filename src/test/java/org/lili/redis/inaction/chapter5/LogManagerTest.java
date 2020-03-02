package org.lili.redis.inaction.chapter5;

import org.junit.Before;
import org.junit.Test;
import org.lili.redis.inaction.base.Base;
import org.lili.redis.inaction.chapter4.RedisManager;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 14:11
 * @description
 * @notes
 */

public class LogManagerTest extends Base {

    private LogManager logManager;

    @Before
    public void setup() {
        logManager = new LogManager();
    }

    @Test
    public void logRecent() {
        for (int i = 0; i < 5; i++) {
            logManager.logRecent("test", "this is message " + i);
        }

        List<String> recent = client.lrange("recent:test:info", 0, -1);
        System.out.println(recent);
        System.out.println("The current recent message log has this many messages: " + recent.size());
    }

    @Test
    public void logCommon() {
        for (int count = 1; count < 6; count++) {
            for (int i = 0; i < count; i ++) {
                logManager.logCommon("test", "message-" + count);
            }
        }
        Set<Tuple> common = client.zrevrangeWithScores("common:test:info", 0, -1);
        System.out.println("The current number of common messages is: " + common.size());
        System.out.println("Those common messages are:");
        for (Tuple tuple : common){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
    }
}