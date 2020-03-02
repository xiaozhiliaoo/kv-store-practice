package org.lili.redis.inaction.chapter4;

import com.alibaba.fastjson.JSON;
import org.lili.redis.inaction.base.Base;

/**
 * @author lili
 * @date 2020/2/29 23:34
 * @description
 * @notes
 */

public class RedisManager extends Base {
    public void info() {
        System.out.println(client.info());
    }

    public void configAll() {
        System.out.println(JSON.toJSONString(client.configGet("*")));
    }
}
