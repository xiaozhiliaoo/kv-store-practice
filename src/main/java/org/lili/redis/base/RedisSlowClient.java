package org.lili.redis.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author lili
 * @date 2020/1/20 14:42
 * @description
 */
public class RedisSlowClient {

    private static Logger log = LoggerFactory.getLogger(RedisSlowClient.class);

    private static JedisPool pool;
    private static RedisSlowClient client;

    //本质就是单例
    static {
        client = new RedisSlowClient();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        pool = new JedisPool(poolConfig, "192.168.197.100",
                6379, 30000, "test", 0);
    }

    public static RedisSlowClient newClient() {
        return client;
    }

    private Jedis getJedis() {
        return pool.getResource();
    }

    public long incr(String key) {
        Jedis conn = getJedis();
        return conn.incr(key);
    }

    public Transaction multi() {
        Jedis conn = getJedis();
        return conn.multi();
    }


    public String acquireLockWithTimeout(String lockName, int acquireTimeout, int lockTimeout) {

        Jedis conn = getJedis();

        String id = UUID.randomUUID().toString();
        lockName = "lock:" + lockName;

        long end = System.currentTimeMillis() + (acquireTimeout * 1000);
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockName, id) >= 1) {
                conn.expire(lockName, lockTimeout);
                return id;
            } else if (conn.ttl(lockName) <= 0) {
                conn.expire(lockName, lockTimeout);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        }
        return null;
    }


    public boolean releaseLock(String lockName, String identifier) {
        Jedis conn = getJedis();
        lockName = "lock:" + lockName;
        while (true) {
            conn.watch(lockName);
            if (identifier.equals(conn.get(lockName))) {
                Transaction trans = conn.multi();
                trans.del(lockName);
                List<Object> result = trans.exec();
                // null response indicates that the transaction was aborted due
                // to the watched key changing.
                if (result == null) {
                    continue;
                }
                return true;
            }

            conn.unwatch();
            break;
        }

        return false;
    }

    public void hset(final String key, final String field, final String value) {
        Jedis conn = getJedis();
        conn.hset(key, field, value);
    }

    public Double zscore(String key, String member) {
        Jedis conn = getJedis();
        return conn.zscore(key, member);
    }

    public String hget(String key, String field) {
        Jedis conn = getJedis();
        return conn.hget(key, field);
    }

    public Long zadd(String key, double score, String member) {
        Jedis conn = getJedis();
        return conn.zadd(key, score, member);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        Jedis conn = getJedis();
        return conn.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<String> zrevrange(String key, long start, long end) {
        Jedis conn = getJedis();
        return conn.zrevrange(key, start, end);
    }

    public Long zcard(String key) {
        Jedis conn = getJedis();
        return conn.zcard(key);
    }

    public Pipeline pipelined() {
        Jedis conn = getJedis();
        return conn.pipelined();
    }
}
