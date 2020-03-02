package org.lili.redis.inaction.chapter5;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 15:45
 * @description
 * @notes
 */

public class StatsManagerTest {

    private StatsManager statsManager;

    @Before
    public void before() {
        statsManager = new StatsManager();
    }

    @Test
    public void updateStats() {
        List<Object> r = null;
        for (int i = 0; i < 5; i++) {
            double value = (Math.random() * 11) + 5;
            r = statsManager.updateStats("ProfilePage", "AccessTime", value);
        }
        System.out.println("We have some aggregate statistics: " + r);
        Map<String, Double> stats = statsManager.getStats("ProfilePage", "AccessTime");
        System.out.println("Which we can also fetch manually:");
        System.out.println(stats);
    }

    @Test
    public void getStats() {
    }
}