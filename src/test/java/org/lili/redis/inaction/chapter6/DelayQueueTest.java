package org.lili.redis.inaction.chapter6;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 21:03
 * @description
 * @notes
 */

public class DelayQueueTest {

    private DelayQueue delayQueue;

    @Before
    public void setUp() throws Exception {
        delayQueue = new DelayQueue();
    }

    @Test
    public void executeLater() {
        for (long delay : new long[]{0, 500, 0, 1500}) {
            delayQueue.executeLater("tqueue", "testfn", new ArrayList<String>(), delay);
        }


    }
}