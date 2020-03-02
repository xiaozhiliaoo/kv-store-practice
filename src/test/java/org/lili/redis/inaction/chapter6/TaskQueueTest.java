package org.lili.redis.inaction.chapter6;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 20:43
 * @description
 * @notes
 */

public class TaskQueueTest {

    private TaskQueue taskQueue;

    @Before
    public void setUp() throws Exception {
        taskQueue = new TaskQueue();
    }

    @Test
    public void sendSoldEmailByQueue() {
        taskQueue.sendSoldEmailByQueue("seller","item1","7.9","buyer");
        taskQueue.sendSoldEmailByQueue("seller","item1","8.0","buyer");
    }

    @Test
    public void processSoldEmailQueue() {
        taskQueue.processSoldEmailQueue();
    }
}