package org.lili.redis.inaction.chapter3;

import org.lili.redis.inaction.base.Base;

/**
 * @author lili
 * @date 2020/2/21 0:08
 * @description
 * @notes
 */
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

        client.append(newKey, "hello ");
        client.append(newKey, "world!");


    }


}
