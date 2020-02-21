package org.lili.redis.inaction.base;

import org.lili.redis.base.RedisClient;

/**
 * @author lili
 * @date 2020/2/15 17:38
 * @description
 * @notes
 */

public class Base {

    /**
     * 本质依赖注入问题
     */
    protected RedisClient client;

    public Base() {
        client = RedisClient.newClient();
    }
}
