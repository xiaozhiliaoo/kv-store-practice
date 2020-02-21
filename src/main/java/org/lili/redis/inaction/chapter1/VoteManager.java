package org.lili.redis.inaction.chapter1;

import org.lili.redis.base.RedisClient;
import org.lili.redis.base.RedisSlowClient;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * @author lili
 * @date 2020/2/13 0:20
 * @description
 * @notes
 */

public class VoteManager extends Base {

    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    /**
     * @param user
     * @param article
     */
    public void articleVote(String user, String article) {
        long cutOff = System.currentTimeMillis() / 1000 - ONE_WEEK_IN_SECONDS;
        boolean isCutoff = client.zscore("time:", article) < cutOff;
        if (isCutoff) {
            return;
        }
        long articleId = Long.parseLong(article.split(":")[1]);
        Long sadd = client.sadd("voted:" + articleId, user);
        if (sadd == 1) {
            //第一次投票
            client.zincrby("score:", VOTE_SCORE, article);
            client.hincrBy(article, "votes", 1);
        }
    }

    public long postArticle(String user, String title, String link) {
        long articleId = client.incr("article:");
        String voted = "voted:" + articleId;
        client.sadd(voted, user);
        client.expire(voted, ONE_WEEK_IN_SECONDS);
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        Map<String, String> articleData = new HashMap<>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        client.hmset(article, articleData);
        client.zadd("score:", now + VOTE_SCORE, article);
        client.zadd("time:", now, article);
        return articleId;
    }

    /**
     * @param page
     * @param order "score:","time:"
     * @return
     */
    public List<Map<String, String>> getArticles(int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        Set<String> ids = client.zrevrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids) {
            Map<String, String> articleData = client.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }
        return articles;
    }

    /**
     * @param articleId
     * @param toAdd     "编程","算法"
     */
    public void addGroups(String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            client.sadd("group:" + group, article);
        }
    }

    public void removeGroups(String articleId, String[] toRemove) {
        String article = "article:" + articleId;
        for (String group : toRemove) {
            client.srem("group:" + group, article);
        }
    }


    /**
     *
     * @param group programming
     * @param page
     * @param order score
     * @return
     */
    public List<Map<String, String>> getGroupArticles(String group, int page, String order) {
        String key = order + group;
        if (!client.exists(key)) {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            //新的key
            client.zinterstore(key, params, "group:" + group, order);
            client.expire(key, 60);
        }
        return getArticles(page, key);
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(group, page, "score:");
    }


}
