package org.lili.redis.inaction.chapter7;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author lili
 * @date 2020/3/2 22:35
 * @description
 * @notes
 */

public class JobSearchManager extends Base {

    private SearchManager searchManager = new SearchManager();


    public void addJob(String jobId, String... requiredSkills) {
        client.sadd("job:" + jobId, requiredSkills);
    }

    @SuppressWarnings("unchecked")
    public boolean isQualified(String jobId, String... candidateSkills) {
        String temp = UUID.randomUUID().toString();
        Transaction trans = client.multi();
        for(String skill : candidateSkills) {
            trans.sadd(temp, skill);
        }
        trans.expire(temp, 5);
        trans.sdiff("job:" + jobId, temp);

        List<Object> response = trans.exec();
        Set<String> diff = (Set<String>)response.get(response.size() - 1);
        return diff.size() == 0;
    }

    public void indexJob(String jobId, String... skills) {
        Transaction trans = client.multi();
        Set<String> unique = new HashSet<String>();
        for (String skill : skills) {
            trans.sadd("idx:skill:" + skill, jobId);
            unique.add(skill);
        }
        trans.zadd("idx:jobs:req", unique.size(), jobId);
        trans.exec();
    }

    public Set<String> findJobs(Jedis conn, String... candidateSkills) {
        String[] keys = new String[candidateSkills.length];
        int[] weights = new int[candidateSkills.length];
        for (int i = 0; i < candidateSkills.length; i++) {
            keys[i] = "skill:" + candidateSkills[i];
            weights[i] = 1;
        }

        Transaction trans = conn.multi();
        String jobScores = searchManager.zunion(trans, 30, new ZParams().weights(null), keys);
        String finalResult =searchManager.zintersect(trans, 30, new ZParams().weights(-1, 1), jobScores, "jobs:req");
        trans.exec();

        return conn.zrangeByScore("idx:" + finalResult, 0, 0);
    }
}
