package org.lili.redis.inaction.chapter2;

import com.google.gson.Gson;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author lili
 * @date 2020/2/20 22:35
 * @description
 * @notes
 */

public class WebManager extends Base {

    /**
     * 获取已经登录用户
     *
     * @param token token
     * @return user
     */
    public String checkToken(String token) {
        return client.hget("login:", token);
    }

    /**
     * @param token token
     * @param user  用户
     * @param item  商品
     */
    public void updateToken(String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        client.hset("login:", token, user);
        //timestamp->score
        client.zadd("recent:", timestamp, token);
        if (item != null) {
            client.zadd("viewed:" + token, timestamp, item);
            //保留最近25个商品  -26 分数最高的25个  0 得分最低的
            client.zremrangeByRank("viewed:" + token, 0, -26);
            //分数-1  被浏览最多的在索引0上，并且分数最少
            client.zincrby("viewed:", -1, item);
        }
    }

    public class CleanSessionsThread extends Thread {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit) {
            client.select(15);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                //recent:个数
                long size = client.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                //最旧的令牌
                Set<String> tokenSet = client.zrange("recent:", 0, (int) (endIndex - 1));
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }
                //删除最旧的令牌
                client.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                client.hdel("login:", tokens);
                client.zrem("recent:", tokens);
            }
        }
    }


    /**
     * @param session
     * @param item    商品ID
     * @param count   商品个数
     */
    public void addToCart(String session, String item, int count) {
        if (count <= 0) {
            client.hdel("cart:" + session, item);
        } else {
            client.hset("cart:" + session, item, String.valueOf(count));
        }
    }

    public class CleanFullSessionsThread extends Thread {
        private int limit;
        private boolean quit;

        public CleanFullSessionsThread(int limit) {
            client.select(15);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                long size = client.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> sessionSet = client.zrange("recent:", 0, (int) (endIndex - 1));
                String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String sess : sessions) {
                    sessionKeys.add("viewed:" + sess);
                    sessionKeys.add("cart:" + sess);
                }

                client.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                client.hdel("login:", sessions);
                client.zrem("recent:", sessions);
            }
        }
    }

    public interface Callback {
        public String call(String request);
    }


    public boolean isDynamic(Map<String, String> params) {
        return params.containsKey("_");
    }

    public String extractItemId(Map<String, String> params) {
        return params.get("item");
    }

    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }

    public String cacheRequest(String request, Callback callback) {
        if (!canCache(request)) {
            //不能被缓存的
            return callback != null ? callback.call(request) : null;
        }

        String pageKey = "cache:" + hashRequest(request);
        String content = client.get(pageKey);

        if (content == null && callback != null) {
            content = callback.call(request);
            //5分钟
            client.setex(pageKey, 300, content);
        }

        return content;
    }

    public boolean canCache(String request) {
        try {
            URL url = new URL(request);
            HashMap<String, String> params = new HashMap<String, String>();
            if (url.getQuery() != null) {
                for (String param : url.getQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            //返回排名
            Long rank = client.zrank("viewed:", itemId);
            //缓存前10000个商品
            return rank != null && rank < 10000;
        } catch (MalformedURLException mue) {
            return false;
        }
    }


    /**
     * @param rowId
     * @param delay
     */
    public void scheduleRowCache(String rowId, int delay) {
        //行id  给定延时时间
        client.zadd("delay:", delay, rowId);
        //行id
        client.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
    }

    public class CacheRowsThread extends Thread {
        private Jedis conn;
        private boolean quit;

        public CacheRowsThread() {
            client.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            Gson gson = new Gson();
            while (!quit) {
                //第一个元素和分数
                Set<Tuple> range = client.zrangeWithScores("schedule:", 0, 0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                //未到调度时间
                if (next == null || next.getScore() > now) {
                    try {
                        sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                String rowId = next.getElement();
                double delay = conn.zscore("delay:", rowId);
                if (delay <= 0) {
                    client.zrem("delay:", rowId);
                    client.zrem("schedule:", rowId);
                    client.del("inv:" + rowId);
                    continue;
                }

                Inventory row = Inventory.get(rowId);
                client.zadd("schedule:", now + delay, rowId);
                //字符串真正存储商品信息
                client.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    /**
     * Inventory 库存
     */
    public static class Inventory {
        private String id;
        private String data;
        private long time;

        private Inventory (String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }

    public class ResacleThread extends Thread {
        private int limit;
        private boolean quit;

        public ResacleThread(int limit) {
            client.select(15);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                //删除排名20000万之后商品,并且将剩余商品浏览次数减半
                client.zremrangeByRank("viewed:",0,-20001);
                ZParams params = new ZParams().weights(0.5);
                //浏览次数降低一半
                client.zinterstore("viewed:", params, "viewed:");
                //5分钟后
                try {
                    TimeUnit.MINUTES.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
