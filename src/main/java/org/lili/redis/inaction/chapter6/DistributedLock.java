package org.lili.redis.inaction.chapter6;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

/**
 * @author lili
 * @date 2020/3/1 17:53
 * @description
 * @notes
 */

public class DistributedLock extends Base {

    public String acquireLock( String lockName) {
        return acquireLock(lockName, 10000);
    }

    public String acquireLock(String lockName, long acquireTimeout) {

        String identifier = UUID.randomUUID().toString();
        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end) {
            //不存在时候，设置值，用来获取锁
            if (client.setnx("lock:" + lockName, identifier) == 1) {
                return identifier;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        //获取不到锁，返回null
        return null;
    }

    public boolean releaseLock(String lockName, String identifier) {
        String lockKey = "lock:" + lockName;
        while (true) {
            client.watch(lockKey);
            if (identifier.equals(client.get(lockKey))) {
                Transaction trans = client.multi();
                trans.del(lockKey);
                List<Object> results = trans.exec();
                if (results == null) {
                    continue;
                }
                return true;
            }
            client.unwatch();
            break;
        }
        return false;
    }

    /**
     * 解决锁持有者奔溃导致的锁不会释放，所以加上超时时间
     * @param lockName
     * @param acquireTimeout
     * @param lockTimeout
     * @return
     */
    public String acquireLockWithTimeout(String lockName, long acquireTimeout, long lockTimeout) {
        String identifier = UUID.randomUUID().toString();
        String lockKey = "lock:" + lockName;
        int lockExpire = (int) (lockTimeout / 1000);
        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end) {
            if (client.setnx(lockKey, identifier) == 1) {
                client.expire(lockKey, lockExpire);
                return identifier;
            }
            if (client.ttl(lockKey) == -1) {
                client.expire(lockKey, lockExpire);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        // null indicates that the lock was not acquired
        return null;
    }


}
