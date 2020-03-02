package org.lili.redis.inaction.chapter5;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lili
 * @date 2020/3/1 15:43
 * @description
 * @notes
 */

public class StatsManager extends Base {

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("EEE MMM dd HH:00:00 yyyy");

    private static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");

    static {
//        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTF+8"));
    }


    public List<Object> updateStats(String context, String type, double value) {
        int timeout = 5000;
        String destination = "stats:" + context + ':' + type;
        String startKey = destination + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end) {
            client.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());
            String existing = client.get(startKey);
            Transaction trans = client.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
                trans.rename(destination, destination + ":last");
                trans.rename(startKey, destination + ":pstart");
                trans.set(startKey, hourStart);
            }

            String tkey1 = UUID.randomUUID().toString();
            String tkey2 = UUID.randomUUID().toString();
            //
            trans.zadd(tkey1, value, "min");
            //
            trans.zadd(tkey2, value, "max");

            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MIN),
                    destination, tkey1);
            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MAX),
                    destination, tkey2);

            //删除临时key
            trans.del(tkey1, tkey2);
            trans.zincrby(destination, 1, "count");
            trans.zincrby(destination, value, "sum");
            trans.zincrby(destination, value * value, "sumsq");

            List<Object> results = trans.exec();
            if (results == null) {
                continue;
            }
            return results.subList(results.size() - 3, results.size());
        }
        return null;
    }


    public Map<String, Double> getStats(String context, String type) {
        String key = "stats:" + context + ':' + type;
        Map<String, Double> stats = new HashMap<String, Double>();
        Set<Tuple> data = client.zrangeWithScores(key, 0, -1);
        for (Tuple tuple : data) {
            stats.put(tuple.getElement(), tuple.getScore());
        }
        stats.put("average", stats.get("sum") / stats.get("count"));
        double numerator = stats.get("sumsq") - Math.pow(stats.get("sum"), 2) / stats.get("count");
        double count = stats.get("count");
        stats.put("stddev", Math.pow(numerator / (count > 1 ? count - 1 : 1), .5));
        return stats;
    }


    public class AccessTimer {
        private long start;

        public void start() {
            start = System.currentTimeMillis();
        }

        public void stop(String context) {
            long delta = System.currentTimeMillis() - start;
            List<Object> stats = updateStats(context, "AccessTime", delta / 1000.0);
            double average = (Double) stats.get(1) / (Double) stats.get(0);
            Transaction trans = client.multi();
            trans.zadd("slowest:AccessTime", average, context);
            //保留最近100条记录
            trans.zremrangeByRank("slowest:AccessTime", 0, -101);
            trans.exec();
        }
    }

}
