package org.lili.redis.inaction.chapter6;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.List;
import java.util.UUID;

/**
 * @author lili
 * @date 2020/3/1 18:11
 * @description
 * @notes
 */

public class SemaphoreManager extends Base {

    public String acquireUnfairSemaphore(String semname, int limit, long timeout) {
        String identifier = UUID.randomUUID().toString();
        Transaction trans = client.multi();
        long now = System.currentTimeMillis();
        trans.zremrangeByScore(semname.getBytes(), "-inf".getBytes(),
                String.valueOf(now - timeout).getBytes());
        trans.zadd(semname, now, identifier);
        //identifier的排名，恰好是个数
        trans.zrank(semname, identifier);
        List<Object> results = trans.exec();
        int result = ((Long) results.get(results.size() - 1)).intValue();
        if (result < limit) {
            //获取信号量成功
            return identifier;
        } else {
            //删除没有获取信号量
            client.zrem(semname, identifier);
        }
        return null;
    }

    public boolean releaseUnfairSemaphore(String semname, String identifier) {
        return client.zrem(semname, identifier) == 1;
    }


    public String acquireFairSemaphore(String semname, int limit, long timeout) {
        String identifier = UUID.randomUUID().toString();
        String czset = semname + ":owner";//zset
        String ctr = semname + ":counter"; //string

        long now = System.currentTimeMillis();
        Transaction trans = client.multi();
        trans.zremrangeByScore(
                semname.getBytes(),
                "-inf".getBytes(),
                String.valueOf(now - timeout).getBytes());
        ZParams params = new ZParams();
        params.weights(1, 0);
        //交集，
        trans.zinterstore(czset, params, czset, semname);
        trans.incr(ctr);
        List<Object> results = trans.exec();
        int counter = ((Long) results.get(results.size() - 1)).intValue();

        trans = client.multi();
        trans.zadd(semname, now, identifier);
        trans.zadd(czset, counter, identifier);
        trans.zrank(czset, identifier);
        results = trans.exec();
        int result = ((Long) results.get(results.size() - 1)).intValue();
        if (result < limit) {
            return identifier;
        }

        trans = client.multi();
        trans.zrem(semname, identifier);
        trans.zrem(czset, identifier);
        trans.exec();
        return null;
    }

    public boolean releaseFairSemaphore(String semname, String identifier) {
        Transaction trans = client.multi();
        trans.zrem(semname, identifier);
        trans.zrem(semname + ":owner", identifier);
        List<Object> results = trans.exec();
        return (Long) results.get(results.size() - 1) == 1;
    }

    public boolean refreshFairSemaphore(String semname, String identifier) {
        long now = System.currentTimeMillis();
        if (client.zadd(semname, now, identifier) == 1) {
            releaseFairSemaphore(semname, identifier);
            return false;
        }
        return true;
    }

    public String acquireFairSemaphoreWithLock(String semname, int limit, long timeout) {
        DistributedLock lock = new DistributedLock();
        String identifier = lock.acquireLock(semname, timeout);
        try {
            if (identifier != null) {
                return acquireFairSemaphore(semname, limit, timeout);
            }
        } finally {
            releaseUnfairSemaphore(semname, identifier);
        }
        return null;
    }

}
