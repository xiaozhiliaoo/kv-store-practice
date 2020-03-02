package org.lili.redis.inaction.chapter5;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lili
 * @date 2020/3/1 16:44
 * @description
 * @notes
 */

public class ConfigManager extends Base {

    private long lastChecked;
    private boolean underMaintenance;

    public boolean isUnderMaintenance() {
        if (lastChecked < System.currentTimeMillis() - 1000) {
            lastChecked = System.currentTimeMillis();
            String flag = client.get("is-under-maintenance");
            underMaintenance = "yes".equals(flag);
        }
        return underMaintenance;
    }


    public void setConfig(String type, String component, Map<String, Object> config) {
        Gson gson = new Gson();
        client.set("config:" + type + ':' + component, gson.toJson(config));
    }

    private static final Map<String, Map<String, Object>> CONFIGS = new HashMap<>();
    private static final Map<String, Long> CHECKED = new HashMap<>();

    @SuppressWarnings("unchecked")
    public Map<String, Object> getConfig(String type, String component) {
        int wait = 1000;
        String key = "config:" + type + ':' + component;

        Long lastChecked = CHECKED.get(key);
        if (lastChecked == null || lastChecked < System.currentTimeMillis() - wait) {
            CHECKED.put(key, System.currentTimeMillis());
            String value = client.get(key);
            Map<String, Object> config = null;
            if (value != null) {
                Gson gson = new Gson();
                config = gson.fromJson(value, new TypeToken<Map<String, Object>>() {}.getType());
            } else {
                config = new HashMap<String, Object>();
            }

            CONFIGS.put(key, config);
        }

        return CONFIGS.get(key);
    }

    public static final Map<String, Jedis> REDIS_CONNECTIONS = new HashMap<>();

    public Jedis redisConnection(String component) {
        Jedis configConn = REDIS_CONNECTIONS.get("config");
        if (configConn == null) {
            configConn = new Jedis("localhost");
            configConn.select(15);
            REDIS_CONNECTIONS.put("config", configConn);
        }

        String key = "config:redis:" + component;
        Map<String, Object> oldConfig = CONFIGS.get(key);
        Map<String, Object> config = getConfig("redis", component);

        if (!config.equals(oldConfig)) {
            Jedis conn = new Jedis("localhost");
            if (config.containsKey("db")) {
                conn.select(((Double) config.get("db")).intValue());
            }
            REDIS_CONNECTIONS.put(key, conn);
        }

        return REDIS_CONNECTIONS.get(key);
    }
}
