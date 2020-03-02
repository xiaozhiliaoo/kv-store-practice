package org.lili.redis.inaction.chapter6;

import com.google.gson.Gson;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author lili
 * @date 2020/3/1 19:29
 * @description
 * @notes
 */

public class DelayQueue extends Base {


    public String executeLater(String queue, String name, List<String> args, long delay) {
        Gson gson = new Gson();
        String identifier = UUID.randomUUID().toString();
        String itemArgs = gson.toJson(args);
        String item = gson.toJson(new String[]{identifier, queue, name, itemArgs});
        if (delay > 0) {
            client.zadd("delayed:", System.currentTimeMillis() + delay, item);
        } else {
            client.rpush("queue:" + queue, item);
        }
        return identifier;
    }


    public class PollQueueThread extends Thread {
        private boolean quit;
        private Gson gson = new Gson();

        public PollQueueThread() {
            client.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            DistributedLock lock = new DistributedLock();
            while (!quit) {
                //从小到大，取出最小的，也就是最早的任务
                Set<Tuple> items = client.zrangeWithScores("delayed:", 0, 0);
                Tuple item = items.size() > 0 ? items.iterator().next() : null;
                if (item == null || item.getScore() > System.currentTimeMillis()) {
                    try {
                        sleep(10);
                    } catch (InterruptedException ie) {
                        Thread.interrupted();
                    }
                    continue;
                }

                String json = item.getElement();
                String[] values = gson.fromJson(json, String[].class);
                String identifier = values[0];
                String queue = values[1];

                String locked = lock.acquireLock(identifier, 5000);
                if (locked == null) {
                    continue;
                }

                if (client.zrem("delayed:", json) == 1) {
                    client.rpush("queue:" + queue, json);
                }

                lock.releaseLock(identifier, locked);
            }
        }
    }


}
