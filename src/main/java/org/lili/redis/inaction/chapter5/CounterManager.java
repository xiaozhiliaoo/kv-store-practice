package org.lili.redis.inaction.chapter5;

import org.javatuples.Pair;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

/**
 * @author lili
 * @date 2020/3/1 14:56
 * @description
 * @notes
 */
public class CounterManager extends Base {

    //精度
    public static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    /**
     * @param name  计数器名字
     * @param count 次数
     * @param now   当前时间
     */
    public void updateCounter(String name, int count, long now) {
        Transaction trans = client.multi();
        for (int prec : PRECISION) {
            //
            long pnow = (now / prec) * prec;
            String hash = String.valueOf(prec) + ':' + name;
            trans.zadd("known:", 0, hash);
            trans.hincrBy("count:" + hash, String.valueOf(pnow), count);
        }
        trans.exec();
    }

    public void updateCounter(String name, int count) {
        updateCounter(name, count, System.currentTimeMillis() / 1000);
    }

    /**
     *
     * @param name 计数器名字
     * @param precision 精度
     * @return
     */
    public List<Pair<Integer, Integer>> getCounter(String name, int precision) {
        String hash = String.valueOf(precision) + ':' + name;
        Map<String, String> data = client.hgetAll("count:" + hash);
        ArrayList<Pair<Integer, Integer>> results = new ArrayList<>();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            results.add(new Pair<>(Integer.parseInt(entry.getKey()), Integer.parseInt(entry.getValue())));
        }
        Collections.sort(results);
        return results;
    }

    public static class CleanCountersThread extends Thread {
        private Jedis client;
        private int sampleCount = 100;
        private boolean quit;
        private long timeOffset; // used to mimic a time in the future.

        public CleanCountersThread(int sampleCount, long timeOffset) {
            this.sampleCount = sampleCount;
            this.timeOffset = timeOffset;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            int passes = 0;
            while (!quit) {
                long start = System.currentTimeMillis() + timeOffset;
                int index = 0;
                while (index < client.zcard("known:")) {
                    Set<String> hashSet = client.zrange("known:", index, index);
                    index++;
                    if (hashSet.size() == 0) {
                        break;
                    }
                    String hash = hashSet.iterator().next();
                    //计数器精度
                    int prec = Integer.parseInt(hash.substring(0, hash.indexOf(':')));
                    int bprec = (int) Math.floor(prec / 60);
                    if (bprec == 0) {
                        bprec = 1;
                    }
                    if ((passes % bprec) != 0) {
                        continue;
                    }

                    String hkey = "count:" + hash;
                    String cutoff = String.valueOf(((System.currentTimeMillis() + timeOffset) / 1000) - sampleCount * prec);
                    ArrayList<String> samples = new ArrayList<String>(client.hkeys(hkey));
                    Collections.sort(samples);
                    int remove = bisectRight(samples, cutoff);

                    if (remove != 0) {
                        client.hdel(hkey, samples.subList(0, remove).toArray(new String[0]));
                        if (remove == samples.size()) {
                            client.watch(hkey);
                            if (client.hlen(hkey) == 0) {
                                Transaction trans = client.multi();
                                trans.zrem("known:", hash);
                                trans.exec();
                                index--;
                            } else {
                                client.unwatch();
                            }
                        }
                    }
                }

                passes++;
                long duration = Math.min((System.currentTimeMillis() + timeOffset) - start + 1000, 60000);
                try {
                    sleep(Math.max(60000 - duration, 1000));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // mimic python's bisect.bisect_right
        public int bisectRight(List<String> values, String key) {
            int index = Collections.binarySearch(values, key);
            return index < 0 ? Math.abs(index) - 1 : index + 1;
        }
    }
}
