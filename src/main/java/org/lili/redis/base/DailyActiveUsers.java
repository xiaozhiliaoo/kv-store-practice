package org.lili.redis.base;

/**
 * @author lili
 * @date 2020/4/6 13:05
 * @description
 * @notes
 */


import org.lili.redis.inaction.base.Base;

import java.util.*;

/**
 * 统计日活用户：https://blog.getspool.com/2011/11/29/fast-easy-realtime-metrics-using-redis-bitmaps/
 * 周活，月活 日活并日活并日活
 */
public class DailyActiveUsers extends Base {

    /**
     * 核心是offset作为userId设计
     * @param action
     * @param date
     * @param userId
     */
    public void log(String action, String date, int userId) {
        client.setbit(action + date, userId, true);
    }

    public int uniqueCount(String action, String date) {
        // setbit(key, offset, value) -> redis.setbit(daily_active_users, user_id, 1) -> redis.setbit(play:yyyy-mm-dd, user_id, 1)
        String key = action + ":" + date;
        BitSet users = BitSet.valueOf(client.get(key.getBytes()));
        return users.cardinality();
    }

    public int uniqueCount(String action, String... dates) {
        BitSet all = new BitSet();
        for (String date : dates) {
            String key = action + ":" + date;
            BitSet users = BitSet.valueOf(client.get(key.getBytes()));
            all.or(users);
        }
        return all.cardinality();
    }

}
