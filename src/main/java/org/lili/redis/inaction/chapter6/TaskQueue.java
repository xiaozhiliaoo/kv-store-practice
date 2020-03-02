package org.lili.redis.inaction.chapter6;

import com.alibaba.fastjson.JSON;
import org.lili.redis.inaction.base.Base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author lili
 * @date 2020/3/1 19:29
 * @description
 * @notes
 */

public class TaskQueue extends Base {

    public void sendSoldEmailByQueue(String seller, String item, String price, String buyer) {
        Map<String, Object> email = new HashMap<>();
        email.put("seller_id", seller);
        email.put("item_id", item);
        email.put("price", price);
        email.put("buyer_id", buyer);
        email.put("time", System.currentTimeMillis());
        client.rpush("queue:email", JSON.toJSONString(email));
    }

    public void processSoldEmailQueue() {
        //blpop[0] = queue:email
        List<String> blpop = client.blpop(30, "queue:email");
        String email = blpop.get(1);
        sendMail(email);
    }

    private void sendMail(String email) {
        System.out.println("email:" + email);
    }

    public void workerWatchQueue(String queue, Function function) {

    }
}
