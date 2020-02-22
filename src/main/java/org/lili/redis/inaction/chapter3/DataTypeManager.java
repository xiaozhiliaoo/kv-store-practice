package org.lili.redis.inaction.chapter3;

import lombok.extern.slf4j.Slf4j;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.ZParams;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lili
 * @date 2020/2/21 0:08
 * @description
 * @notes
 */
@Slf4j
public class DataTypeManager extends Base {

    public void string() {
        String key = "key";
        System.out.println(client.get(key));
        System.out.println(client.incr(key));
        System.out.println(client.incrBy(key, 15));
        System.out.println(client.decrBy(key, 5));
        System.out.println(client.get(key));
        System.out.println(client.set(key, "13"));
        System.out.println(client.incr(key));
    }


    public void byteString() {
        String newKey = "newStringkey";
        String anotherKey = "anotherKey";
        System.out.println(client.append(newKey, "hello "));
        System.out.println(client.append(newKey, "world!"));
        //hello world!
        System.out.println(client.substr(newKey, 3, 7)); //'lo wo'
        System.out.println(client.setrange(newKey, 0, "H"));
        System.out.println(client.setrange(newKey, 6, "W"));
        System.out.println(client.get(newKey));//Hello World!
        client.setbit(anotherKey, 2, true);
        client.setbit(anotherKey, 7, true);
        System.out.println(client.get(anotherKey));
    }

    public void list() {
        String k = "list-key";
        System.out.println(client.rpush(k, "last"));
        System.out.println(client.lpush(k, "first"));//返回长度
        System.out.println(client.rpush(k, "new last"));
        System.out.println(client.lrange(k, 0, -1));
        System.out.println(client.lpop(k));
        System.out.println(client.lpop(k));
        System.out.println(client.lrange(k, 0, -1));
        System.out.println(client.rpush(k, "a", "b", "c"));
        System.out.println(client.lrange(k, 0, -1));
        System.out.println(client.ltrim(k, 2, -1));
        System.out.println(client.lrange(k, 0, -1));
    }

    public void blist() {
        String k1 = "list";
        String k2 = "list2";
        System.out.println(client.rpush(k1, "item1"));
        System.out.println(client.rpush(k1, "item2"));
        System.out.println(client.rpush(k2, "item3"));
        System.out.println(client.brpoplpush(k2, k1, 1));
        System.out.println(client.lrange(k1, 0, -1));
        System.out.println(client.brpoplpush(k1, k2, 1));
        System.out.println(client.blpop(1, k1, k2));
        System.out.println(client.blpop(1, k1, k2));
        System.out.println(client.blpop(1, k1, k2));
        System.out.println(client.blpop(1, k1, k2));
    }

    public void set() {
        String k1 = "set-key";
        String k2 = "set-key2";
        log.info("{}", client.sadd(k1, "a", "b", "c"));
        log.info("{}", client.srem(k1, "c", "d"));
        log.info("{}", client.srem("c", "d"));
        log.info("{}", client.scard(k1));
        log.info("{}", client.smembers(k1));
        log.info("{}", client.smove(k1, k2, "a"));
        log.info("{}", client.smove(k1, k2, "c"));
        log.info("{}", client.smembers(k2));
    }

    public void mathSet() {
        String k1 = "set-key1";
        String k2 = "set-key2";
        System.out.println(client.sadd(k1, "a", "b", "c", "d"));
        System.out.println(client.sadd(k2, "c", "d", "e", "f"));
        System.out.println(client.sdiff(k1, k2));
        System.out.println(client.sinter(k1, k2));
        System.out.println(client.sunion(k1, k2));
    }

    public void hash() {
        String k1 = "hash-key";
        String k2 = "hash-key2";
        Map<String, String> v1 = new HashMap<>();
        v1.put("k1", "v1");
        v1.put("k2", "v2");
        v1.put("k3", "v3");
        System.out.println(client.hmset(k1, v1));
        System.out.println(client.hmget(k1, "k2", "k3"));
        System.out.println(client.hlen(k1));
        System.out.println(client.hdel(k1, "k1", "k3"));
    }

    public void zset() {
        String k = "zset-key";
        Map<String, Double> scores = new HashMap<>();
        scores.put("a", 3d);
        scores.put("b", 2d);
        scores.put("c", 1d);
        System.out.println(client.zadd(k, scores));
        System.out.println(client.zcard(k));
        System.out.println(client.zincrby(k, 3d, "c"));
        System.out.println(client.zscore(k, "b"));
        System.out.println(client.zrank(k, "c"));
        System.out.println(client.zcount(k, 0d, 3d));
        System.out.println(client.zrem(k, "b"));
        System.out.println(client.zrevrange(k, 0, -1));

    }

    public void zsetMuitl() {
        String k1 = "zset-1";
        String k2 = "zset-2";
        String ki = "zset-i";
        String ku = "zset-u";
        String ku2 = "zset-u2";
        String ks = "set-1";
        client.zadd(k1, new HashMap<String,Double>(){{
            put("a",1d);
            put("b",2d);
            put("c",3d);
        }});

        client.zadd(k2, new HashMap<String, Double>(){{
            put("b",4d);
            put("c",1d);
            put("d",0d);
        }});
        ZParams sum = new ZParams().aggregate(ZParams.Aggregate.SUM);
        client.zinterstore(ki,sum , k1,k2);
        System.out.println(client.zrangeWithScores(ki, 0, -1));

        client.zunionstore(ku, new ZParams().aggregate(ZParams.Aggregate.MIN), k1,k2);
        System.out.println(client.zrangeWithScores(ku, 0, -1));

        client.sadd(ks,"a","d");

        client.zunionstore(ku2, new ZParams().aggregate(ZParams.Aggregate.SUM),k1,k2,ks);
        System.out.println(client.zrangeWithScores(ku2, 0, -1));


    }


    }
