package org.lili.redis.inaction.chapter4;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lili
 * @date 2020/3/1 0:02
 * @description
 * @notes
 */

public class MarketManager extends Base {

    public void createUser() {
        Map<String, String> map = new HashMap<>();
        map.put("name", "Frank");
        map.put("funds", "43");
        client.hmset("users:17", map);
        Map<String, String> map2 = new HashMap<>();
        map2.put("name", "Bill");
        map2.put("funds", "125");
        client.hmset("users:27", map2);
    }

    public void createInventory() {
        client.sadd("inventory:17", "ItemL", "ItemM", "ItemN");
        client.sadd("inventory:27", "ItemO", "ItemP", "ItemQ");
    }

    /**
     * 把商品放在市场上销售
     *
     * @param itemId
     * @param sellerId
     * @param price
     * @return
     */
    public boolean listItem(String itemId, String sellerId, double price) {
        //包裹
        String inventory = "inventory:" + sellerId;
        String item = itemId + '.' + sellerId;
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end) {
            client.watch(inventory);
            //在用户的包裹里面
            if (!client.sismember(inventory, itemId)) {
                client.unwatch();
                return false;
            }

            Transaction trans = client.multi();
            trans.zadd("market:", price, item);
            trans.srem(inventory, itemId);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }


    /**
     * 购买商品
     *
     * @param buyerId
     * @param itemId
     * @param sellerId
     * @param lprice
     * @return
     */
    public boolean purchaseItem(String buyerId, String itemId, String sellerId, double lprice) {

        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        String item = itemId + '.' + sellerId;
        String inventory = "inventory:" + buyerId;
        long end = System.currentTimeMillis() + 10000;

        while (System.currentTimeMillis() < end) {
            //监听多个key
            client.watch("market:", buyer);

            double price = client.zscore("market:", item);
            double funds = Double.parseDouble(client.hget(buyer, "funds"));
            if (price != lprice || price > funds) {
                client.unwatch();
                return false;
            }

            Transaction trans = client.multi();
            trans.hincrBy(seller, "funds", (int) price);
            trans.hincrBy(buyer, "funds", (int) -price);
            trans.sadd(inventory, itemId);
            trans.zrem("market:", item);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null) {
                continue;
            }
            return true;
        }

        return false;
    }

    public void updateToken(String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        client.hset("login:", token, user);
        client.zadd("recent:", timestamp, token);
        if (item != null) {
            client.zadd("viewed:" + token, timestamp, item);
            client.zremrangeByRank("viewed:" + token, 0, -26);
            client.zincrby("viewed:", -1, item);
        }
    }

    public void updateTokenPipeline(String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        Pipeline pipe = client.pipelined();
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null) {
            pipe.zadd("viewed:" + token, timestamp, item);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item);
        }
        pipe.exec();
    }

    public void benchmarkUpdateToken(int duration) {
        try {
            @SuppressWarnings("rawtypes")
            Class[] args = new Class[]{String.class, String.class, String.class};
            Method[] methods = new Method[]{
                    this.getClass().getDeclaredMethod("updateToken", args),
                    this.getClass().getDeclaredMethod("updateTokenPipeline", args),
            };
            for (Method method : methods) {
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + (duration * 1000);
                while (System.currentTimeMillis() < end) {
                    count++;
                    method.invoke(this, "token", "user", "item");
                }
                long delta = System.currentTimeMillis() - start;
                System.out.println(
                        method.getName() + ' ' +
                                count + ' ' +
                                (delta / 1000) + ' ' +
                                (count / (delta / 1000)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
