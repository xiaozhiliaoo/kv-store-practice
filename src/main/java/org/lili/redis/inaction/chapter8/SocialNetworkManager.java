package org.lili.redis.inaction.chapter8;

import com.google.common.base.Joiner;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.Method;
import java.util.*;

/**
 * @author lili
 * @date 2020/3/2 23:43
 * @description
 * @notes
 */

public class SocialNetworkManager extends Base {

    private static int HOME_TIMELINE_SIZE = 1000;
    private static int POSTS_PER_PASS = 1000;
    private static int REFILL_USERS_STEP = 50;
    
    public Long createUser(String login, String userName) {
        String llogin = login.toLowerCase();
        String lockName = Joiner.on(":").join("user", llogin);
        String lock = client.acquireLockWithTimeout(lockName, 10, 1);
        if (lock == null) {
            return -1L;
        }
        long id = client.incr("user:id:");
        Transaction trans = client.multi();
        client.hset("users:", llogin, String.valueOf(id));
        Map<String, String> values = new HashMap<String, String>();
        values.put("login", login);
        values.put("id", String.valueOf(id));
        values.put("name", userName);
        values.put("followers", "0");
        values.put("following", "0");
        values.put("posts", "0");
        values.put("signup", String.valueOf(System.currentTimeMillis()));
        trans.hmset("user:" + id, values);
        trans.exec();
        client.releaseLock("user:" + llogin, lock);
        return id;
    }

    
    public Long createStatus(Integer uid, String message, Map<String, String> data) {
        Transaction trans = client.multi();
        trans.hget("user:" + uid, "login");
        trans.incr("status:id:");
        List<Object> response = trans.exec();
        String login = (String) response.get(0);
        long id = (Long) response.get(1);

        if (login == null) {
            return -1L;
        }

        if (data == null) {
            data = new HashMap<String, String>();
        }
        data.put("message", message);
        data.put("posted", String.valueOf(System.currentTimeMillis()));
        data.put("id", String.valueOf(id));
        data.put("uid", String.valueOf(uid));
        data.put("login", login);

        trans = client.multi();
        trans.hmset("status:" + id, data);
        trans.hincrBy("user:" + uid, "posts", 1);
        trans.exec();
        return id;
    }

    
    public void followUser(Integer uid, String otherUid) {
        String fkey1 = "following:" + uid;
        String fkey2 = "followers:" + otherUid;
        if (client.zscore(fkey1, String.valueOf(otherUid)) != null) {
            return;
        }

        long now = System.currentTimeMillis();

        Transaction trans = client.multi();
        trans.zadd(fkey1, now, String.valueOf(otherUid));
        trans.zadd(fkey2, now, String.valueOf(uid));
        trans.zcard(fkey1);
        trans.zcard(fkey2);
        trans.zrevrangeWithScores("profile:" + otherUid, 0, HOME_TIMELINE_SIZE - 1);

        List<Object> response = trans.exec();
        long following = (Long) response.get(response.size() - 3);
        long followers = (Long) response.get(response.size() - 2);
        Set<Tuple> statuses = (Set<Tuple>) response.get(response.size() - 1);

        trans = client.multi();
        trans.hset("user:" + uid, "following", String.valueOf(following));
        trans.hset("user:" + otherUid, "followers", String.valueOf(followers));
        if (statuses.size() > 0) {
            for (Tuple status : statuses) {
                trans.zadd("home:" + uid, status.getScore(), status.getElement());
            }
        }
        trans.zremrangeByRank("home:" + uid, 0, 0 - HOME_TIMELINE_SIZE - 1);
        trans.exec();
    }

    
    public void unfollowUser(Integer uid, String otherUid) {
        String fkey1 = "following:" + uid;
        String fkey2 = "followers:" + otherUid;
        if (client.zscore(fkey1, String.valueOf(otherUid)) == null) {
            return;
        }
        Transaction trans = client.multi();
        trans.zrem(fkey1, String.valueOf(otherUid));
        trans.zrem(fkey2, String.valueOf(uid));
        trans.zcard(fkey1);
        trans.zcard(fkey2);
        trans.zrevrange("profile:" + otherUid, 0, HOME_TIMELINE_SIZE - 1);
        List<Object> response = trans.exec();
        long following = (Long) response.get(response.size() - 3);
        long followers = (Long) response.get(response.size() - 2);
        Set<String> statuses = (Set<String>) response.get(response.size() - 1);
        trans = client.multi();
        trans.hset("user:" + uid, "following", String.valueOf(following));
        trans.hset("user:" + otherUid, "followers", String.valueOf(followers));
        if (statuses.size() > 0) {
            for (String status : statuses) {
                trans.zrem("home:" + uid, status);
            }
        }
        trans.exec();
    }

    
    public Long postStatus(Integer uid, String message, Map<String, String> data) {
        long id = createStatus(uid, message, data);
        if (id == -1L) {
            return -1L;
        }
        String postedString = client.hget("status:" + id, "posted");
        if (postedString == null) {
            return -1L;
        }
        long posted = Long.parseLong(postedString);
        client.zadd("profile:" + uid, posted, String.valueOf(id));
        syndicateStatus(uid, id, posted, 0);
        return id;
    }

    
    public boolean deleteStatus(Integer uid, Integer statusId) {
        String key = "status:" + statusId;
        String lock = client.acquireLockWithTimeout(key, 1, 10);
        if (lock == null) {
            return false;
        }

        try {
            if (!String.valueOf(uid).equals(client.hget(key, "uid"))) {
                return false;
            }

            Transaction trans = client.multi();
            trans.del(key);
            trans.zrem("profile:" + uid, String.valueOf(statusId));
            trans.zrem("home:" + uid, String.valueOf(statusId));
            trans.hincrBy("user:" + uid, "posts", -1);
            trans.exec();

            return true;
        } finally {
            client.releaseLock(key, lock);
        }
    }

    
    public List<Map<String, String>> getStatusMessages(Integer uid) {
        return getStatusMessages(uid, 1, 30);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getStatusMessages(long uid, int page, int count) {
        Set<String> statusIds = client.zrevrange("home:" + uid, (page - 1) * count, page * count - 1);

        Transaction trans = client.multi();
        for (String id : statusIds) {
            trans.hgetAll("status:" + id);
        }

        List<Map<String, String>> statuses = new ArrayList<Map<String, String>>();
        for (Object result : trans.exec()) {
            Map<String, String> status = (Map<String, String>) result;
            if (status != null && status.size() > 0) {
                statuses.add(status);
            }
        }
        return statuses;
    }

    private void syndicateStatus(long uid, long postId, long postTime, double start) {
        Set<Tuple> followers = client.zrangeByScoreWithScores(
                "followers:" + uid,
                String.valueOf(start), "inf",
                0, POSTS_PER_PASS);

        Transaction trans = client.multi();
        for (Tuple tuple : followers) {
            String follower = tuple.getElement();
            start = tuple.getScore();
            trans.zadd("home:" + follower, postTime, String.valueOf(postId));
            trans.zrange("home:" + follower, 0, -1);
            trans.zremrangeByRank(
                    "home:" + follower, 0, 0 - HOME_TIMELINE_SIZE - 1);
        }
        trans.exec();

        if (followers.size() >= POSTS_PER_PASS) {
            try {
                Method method = getClass().getDeclaredMethod("syndicateStatus", Jedis.class, Long.TYPE, Long.TYPE, Long.TYPE, Double.TYPE);
                executeLater("default", method, uid, postId, postTime, start);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void refillTimeline(String incoming, String timeline) {
        refillTimeline(incoming, timeline, 0);
    }

    public void refillTimeline(String incoming, String timeline, double start) {
        if (start == 0 && client.zcard(timeline) >= 750) {
            return;
        }

        Set<Tuple> users = client.zrangeByScoreWithScores(
                incoming, String.valueOf(start), "inf", 0, REFILL_USERS_STEP);

        Pipeline pipeline = client.pipelined();
        for (Tuple tuple : users) {
            String uid = tuple.getElement();
            start = tuple.getScore();
            pipeline.zrevrangeWithScores("profile:" + uid, 0, HOME_TIMELINE_SIZE - 1);
        }

        List<Object> response = pipeline.syncAndReturnAll();
        List<Tuple> messages = new ArrayList<Tuple>();
        for (Object results : response) {
            messages.addAll((Set<Tuple>) results);
        }

        Collections.sort(messages);
        messages = messages.subList(0, HOME_TIMELINE_SIZE);

        Transaction trans = client.multi();
        if (messages.size() > 0) {
            for (Tuple tuple : messages) {
                trans.zadd(timeline, tuple.getScore(), tuple.getElement());
            }
        }
        trans.zremrangeByRank(timeline, 0, 0 - HOME_TIMELINE_SIZE - 1);
        trans.exec();

        if (users.size() >= REFILL_USERS_STEP) {
            try {
                Method method = getClass().getDeclaredMethod(
                        "refillTimeline", String.class, String.class, Double.TYPE);
                executeLater("default", method, incoming, timeline, start);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void cleanTimelines(long uid, long statusId) {
        cleanTimelines(uid, statusId, 0, false);
    }

    public void cleanTimelines(long uid, long statusId, double start, boolean onLists) {
        String key = "followers:" + uid;
        String base = "home:";
        if (onLists) {
            key = "list:out:" + uid;
            base = "list:statuses:";
        }
        Set<Tuple> followers = client.zrangeByScoreWithScores(
                key, String.valueOf(start), "inf", 0, POSTS_PER_PASS);

        Transaction trans = client.multi();
        for (Tuple tuple : followers) {
            start = tuple.getScore();
            String follower = tuple.getElement();
            trans.zrem(base + follower, String.valueOf(statusId));
        }
        trans.exec();

        Method method = null;
        try {
            method = getClass().getDeclaredMethod(
                    "cleanTimelines", Jedis.class,
                    Long.TYPE, Long.TYPE, Double.TYPE, Boolean.TYPE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (followers.size() >= POSTS_PER_PASS) {
            executeLater("default", method, uid, statusId, start, onLists);

        } else if (!onLists) {
            executeLater("default", method, uid, statusId, 0, true);
        }
    }


    public void executeLater(String queue, Method method, Object... args) {
        MethodThread thread = new MethodThread(this, method, args);
        thread.start();
    }

    public class MethodThread extends Thread {
        private Object instance;
        private Method method;
        private Object[] args;

        public MethodThread(Object instance, Method method, Object... args) {
            this.instance = instance;
            this.method = method;
            this.args = args;
        }

        
        public void run() {
            Object[] args = new Object[this.args.length + 1];
            System.arraycopy(this.args, 0, args, 1, this.args.length);
            args[0] = client;

            try {
                method.invoke(instance, args);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
