package org.lili.redis.inaction.chapter6;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 18:29
 * @description
 * @notes
 */

public class SemaphoreManagerTest {

    private SemaphoreManager semaphoreManager;

    @Before
    public void setUp() throws Exception {
        semaphoreManager = new SemaphoreManager();
    }


    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void acquireUnfairSemaphore() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            semaphoreManager.acquireUnfairSemaphore("testsem", 3, 1000);
        }

        String result = semaphoreManager.acquireUnfairSemaphore("testsem", 3, 1000);
        System.out.println("four client to get:" + result);

        Thread.sleep(2000);
        System.out.println("Can we get one?");
        String id = semaphoreManager.acquireUnfairSemaphore("testsem", 3, 1000);
        semaphoreManager.releaseUnfairSemaphore("testsem", id);
    }

    @Test
    public void testAcquireUnfairSemaphore() {
    }

    @Test
    public void acquireFairSemaphore() {
    }

    @Test
    public void releaseFairSemaphore() {
    }

    @Test
    public void releaseUnfairSemaphore() {
    }
}