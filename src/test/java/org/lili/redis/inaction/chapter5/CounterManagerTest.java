package org.lili.redis.inaction.chapter5;

import com.alibaba.fastjson.JSON;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 15:10
 * @description
 * @notes
 */

public class CounterManagerTest {

    private CounterManager counterManager;

    @Before
    public void before() {
        counterManager = new CounterManager();
    }

    @Test
    public void updateCounter() {
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 10; i++) {
            int count = (int)(Math.random() * 5) + 1;
            counterManager.updateCounter("test", count, now + i);
        }
        List<Pair<Integer, Integer>> test = counterManager.getCounter("test", 5);
        System.out.println(JSON.toJSONString(test));
    }

    @Test
    public void getCounter() throws InterruptedException {
        List<Pair<Integer, Integer>> test = counterManager.getCounter("test", 5);
        System.out.println(JSON.toJSONString(test));

        CounterManager.CleanCountersThread thread = new
                CounterManager.CleanCountersThread(0, 2 * 86400000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        thread.interrupt();
    }
}