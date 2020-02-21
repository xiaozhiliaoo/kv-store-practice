package org.lili.redis.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.*;

/**
 * @author lili
 * @date 2020/1/20 14:42
 * @description
 */
public class RedisClient {

    private static Logger log = LoggerFactory.getLogger(RedisClient.class);

    private static JedisPool pool;
    private static Jedis conn;
    private static RedisClient client;

    //本质就是单例
    static {
        client = new RedisClient();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        pool = new JedisPool(poolConfig, "192.168.197.100",
                6379, 30000, "test", 0);
        conn = pool.getResource();
    }

    public static RedisClient newClient() {
        return client;
    }


    public Transaction multi() {
        return conn.multi();
    }

    public Map<String, String> hgetAll(String key) {
        Map<String, String> result = null;
        try {
            result = conn.hgetAll(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String acquireLockWithTimeout(String lockName, int acquireTimeout, int lockTimeout) {


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


    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return conn.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<String> zrevrange(String key, long start, long end) {
        return conn.zrevrange(key, start, end);
    }


    public Pipeline pipelined() {
        return conn.pipelined();
    }

    public String set(String key, String value) {
        String result = null;
        try {
            result = conn.set(key, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }


    /**
     * 获取单个key
     *
     * @param key
     * @return
     */
    public String get(String key) {
        String result = null;
        try {
            result = conn.get(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Boolean exists(String key) {
        Boolean result = false;
        try {
            result = conn.exists(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String type(String key) {
        String result = null;
        try {
            result = conn.type(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long expire(String key, int seconds) {
        Long result = null;
        try {
            result = conn.expire(key, seconds);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 在某个时间点失效
     *
     * @param key
     * @param unixTime
     * @return
     */
    public Long expireAt(String key, long unixTime) {
        Long result = null;
        try {
            result = conn.expireAt(key, unixTime);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long ttl(String key) {
        Long result = null;
        try {
            result = conn.ttl(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public boolean setbit(String key, long offset, boolean value) {
        boolean result = false;
        try {
            result = conn.setbit(key, offset, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public boolean getbit(String key, long offset) {
        boolean result = false;
        try {
            result = conn.getbit(key, offset);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public long setrange(String key, long offset, String value) {
        long result = 0;
        try {
            result = conn.setrange(key, offset, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String getrange(String key, long startOffset, long endOffset) {
        String result = null;
        try {
            result = conn.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String getSet(String key, String value) {
        String result = null;
        try {
            result = conn.getSet(key, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long setnx(String key, String value) {
        Long result = null;
        try {
            result = conn.setnx(key, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String setex(String key, int seconds, String value) {
        String result = null;
        try {
            result = conn.setex(key, seconds, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long decrBy(String key, long integer) {
        Long result = null;
        try {
            result = conn.decrBy(key, integer);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long decr(String key) {
        Long result = null;
        try {
            result = conn.decr(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long incrBy(String key, long integer) {
        Long result = null;
        try {
            result = conn.incrBy(key, integer);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long incr(String key) {
        Long result = null;
        try {
            result = conn.incr(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long append(String key, String value) {
        Long result = null;
        try {
            result = conn.append(key, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String substr(String key, int start, int end) {
        String result = null;
        try {
            result = conn.substr(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long hset(String key, String field, String value) {
        Long result = null;
        try {
            result = conn.hset(key, field, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String hget(String key, String field) {

        String result = null;
        try {
            result = conn.hget(key, field);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long hsetnx(String key, String field, String value) {
        Long result = null;
        try {
            result = conn.hsetnx(key, field, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long hlen(String key, String field, String value) {
        Long result = null;
        try {
            result = conn.hlen(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String hmset(String key, Map<String, String> hash) {
        String result = null;
        try {
            result = conn.hmset(key, hash);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public List<String> hmget(String key, String... fields) {
        List<String> result = null;
        try {
            result = conn.hmget(key, fields);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long hincrBy(String key, String field, long value) {
        Long result = null;
        try {
            result = conn.hincrBy(key, field, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Boolean hexists(String key, String field) {
        Boolean result = false;
        try {
            result = conn.hexists(key, field);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long del(String key) {
        Long result = null;
        try {
            result = conn.del(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long del(final String... keys) {
        Long result = null;
        try {
            result = conn.del(keys);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long zrem(final String key, final String... members) {
        Long result = null;
        try {
            result = conn.hdel(key, members);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long hdel(final String key, final String... fields) {
        Long result = null;
        try {
            result = conn.hdel(key, fields);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long hdel(String key, String field) {
        Long result = null;
        try {
            result = conn.hdel(key, field);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long hlen(String key) {
        Long result = null;
        try {
            result = conn.hlen(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> hkeys(String key) {
        Set<String> result = null;
        try {
            result = conn.hkeys(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public List<String> hvals(String key) {
        List<String> result = null;
        try {
            result = conn.hvals(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }


    // ================list ====== l表示 list,l表示left, r表示right====================
    public Long rpush(String key, String string) {
        Long result = null;
        try {
            result = conn.rpush(key, string);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long lpush(String key, String string) {
        Long result = null;
        try {
            result = conn.lpush(key, string);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long llen(String key) {
        Long result = null;
        try {
            result = conn.llen(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public List<String> lrange(String key, long start, long end) {
        List<String> result = null;
        try {
            result = conn.lrange(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String ltrim(String key, long start, long end) {
        String result = null;
        try {
            result = conn.ltrim(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String lindex(String key, long index) {
        String result = null;
        try {
            result = conn.lindex(key, index);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String lset(String key, long index, String value) {
        String result = null;
        try {
            result = conn.lset(key, index, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long lrem(String key, long count, String value) {
        Long result = null;
        try {
            result = conn.lrem(key, count, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String lpop(String key) {
        String result = null;
        try {
            result = conn.lpop(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String rpop(String key) {
        String result = null;
        try {
            result = conn.rpop(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    // return 1 add a not exist value ,
    // return 0 add a exist value
    public Long sadd(String key, String member) {
        Long result = null;
        try {
            result = conn.sadd(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long sadd(String key, String... member) {
        Long result = null;
        try {
            result = conn.sadd(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> smembers(String key) {
        Set<String> result = null;
        try {
            result = conn.smembers(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long srem(String key, String member) {
        Long result = null;
        try {
            result = conn.srem(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String spop(String key) {
        String result = null;
        try {
            result = conn.spop(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long scard(String key) {
        Long result = null;
        try {
            result = conn.scard(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Boolean sismember(String key, String member) {
        Boolean result = null;
        try {
            result = conn.sismember(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String srandmember(String key) {
        String result = null;
        try {
            result = conn.srandmember(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long zadd(String key, double score, String member) {
        Long result = null;
        try {
            result = conn.zadd(key, score, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> zrange(String key, int start, int end) {
        Set<String> result = null;
        try {
            result = conn.zrange(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long zrem(String key, String member) {
        Long result = null;
        try {
            result = conn.zrem(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Double zincrby(String key, double score, String member) {
        Double result = null;
        try {
            result = conn.zincrby(key, score, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long zrank(String key, String member) {
        Long result = null;
        try {
            result = conn.zrank(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long zrevrank(String key, String member) {
        Long result = null;
        try {
            result = conn.zrevrank(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> zrevrange(String key, int start, int end) {
        Set<String> result = null;
        try {
            result = conn.zrevrange(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 按 score 值递增(从小到大)来排序
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<Tuple> zrangeWithScores(String key, int start, int end) {
        Set<Tuple> result = null;
        try {
            result = conn.zrangeWithScores(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * score值递减(从大到小)来排列 start:0 end:1 取分值最大值
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
        Set<Tuple> result = null;
        try {
            result = conn.zrevrangeWithScores(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public String zGetMaxScoreElement(String key) {
        try {
            Set<Tuple> result = conn.zrevrangeWithScores(key, 0, 1);
            ArrayList<Tuple> tuples = new ArrayList<>(result);
            Tuple tuple = tuples.get(0);
            return tuple.getElement();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public Long zcard(String key) {
        Long result = null;
        try {
            result = conn.zcard(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Double zscore(String key, String member) {
        Double result = null;
        try {
            result = conn.zscore(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public List<String> sort(String key) {
        List<String> result = null;
        try {
            result = conn.sort(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
        List<String> result = null;
        try {
            result = conn.sort(key, sortingParameters);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long zcount(String key, double min, double max) {
        Long result = null;
        try {
            result = conn.zcount(key, min, max);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        Set<String> result = null;
        try {
            result = conn.zrangeByScore(key, min, max);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min) {
        Set<String> result = null;
        try {
            result = conn.zrevrangeByScore(key, max, min);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        Set<String> result = null;
        try {
            result = conn.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        Set<String> result = null;
        try {
            result = conn.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        Set<Tuple> result = null;
        try {
            result = conn.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        Set<Tuple> result = null;
        try {
            result = conn.zrevrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        Set<Tuple> result = null;
        try {
            result = conn.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        Set<Tuple> result = null;
        try {
            result = conn.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 返回删除元素的个数
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Long zremrangeByRank(String key, int start, int end) {
        Long result = null;
        try {
            result = conn.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Long zremrangeByScore(String key, double start, double end) {
        Long result = null;
        try {
            result = conn.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public Double incrByFloat(String key, double integer) {
        Double result = null;
        try {
            result = conn.incrByFloat(key, integer);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return result;
    }

    public void redisPublish(String channel, String message) throws Exception {
        conn.publish(channel, message);
    }

    public long zinterstore(String key, ZParams params, String... sets) {
        return conn.zinterstore(key, params, sets);
    }

    public void select(int i) {
        conn.select(i);
    }
}
