package org.lili.redis.inaction.chapter7;

import org.javatuples.Pair;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lili
 * @date 2020/3/2 22:35
 * @description 广告定向引擎
 * @notes
 */

public class AdvertTargetEngine extends Base {

    private SearchManager searchManager = new SearchManager();

    public enum Ecpm {
        CPC, CPA, CPM
    }

    public double toEcpm(Ecpm type, double views, double avg, double value) {
        switch (type) {
            case CPC:
            case CPA:
                return 1000. * value * avg / views;
            case CPM:
                return value;
        }
        return value;
    }

    private Map<Ecpm, Double> AVERAGE_PER_1K = new HashMap<Ecpm, Double>();

    public void indexAd(String id, String[] locations, String content, Ecpm type, double value) {
        Transaction trans = client.multi();

        for (String location : locations) {
            trans.sadd("idx:req:" + location, id);
        }

        Set<String> words = searchManager.tokenize(content);
        for (String word : searchManager.tokenize(content)) {
            trans.zadd("idx:" + word, 0, id);
        }


        double avg = AVERAGE_PER_1K.containsKey(type) ? AVERAGE_PER_1K.get(type) : 1;
        double rvalue = toEcpm(type, 1000, avg, value);

        trans.hset("type:", id, type.name().toLowerCase());
        trans.zadd("idx:ad:value:", rvalue, id);
        trans.zadd("ad:base_value:", value, id);
        for (String word : words) {
            trans.sadd("terms:" + id, word);
        }
        trans.exec();
    }

    @SuppressWarnings("unchecked")
    public Pair<Long, String> targetAds(Jedis client, String[] locations, String content) {
        Transaction trans = client.multi();

        String matchedAds = matchLocation(trans, locations);

        String baseEcpm = searchManager.zintersect(
                trans, 30, new ZParams().weights(0, 1), matchedAds, "ad:value:");

        Pair<Set<String>, String> result = finishScoring(
                trans, matchedAds, baseEcpm, content);

        trans.incr("ads:served:");
        trans.zrevrange("idx:" + result.getValue1(), 0, 0);

        List<Object> response = trans.exec();
        long targetId = (Long) response.get(response.size() - 2);
        Set<String> targetedAds = (Set<String>) response.get(response.size() - 1);

        if (targetedAds.size() == 0) {
            return new Pair<Long, String>(null, null);
        }

        String adId = targetedAds.iterator().next();
        recordTargetingResult(targetId, adId, result.getValue0());

        return new Pair<Long, String>(targetId, adId);
    }

    public String matchLocation(Transaction trans, String[] locations) {
        String[] required = new String[locations.length];
        for (int i = 0; i < locations.length; i++) {
            required[i] = "req:" + locations[i];
        }
        return searchManager.union(trans, 300, required);
    }

    public Pair<Set<String>, String> finishScoring(Transaction trans, String matched, String base, String content) {
        Map<String, Integer> bonusEcpm = new HashMap<String, Integer>();
        Set<String> words = searchManager.tokenize(content);
        for (String word : words) {
            String wordBonus = searchManager.zintersect(
                    trans, 30, new ZParams().weights(0, 1), matched, word);
            bonusEcpm.put(wordBonus, 1);
        }

        if (bonusEcpm.size() > 0) {

            String[] keys = new String[bonusEcpm.size()];
            int[] weights = new int[bonusEcpm.size()];
            int index = 0;
            for (Map.Entry<String, Integer> bonus : bonusEcpm.entrySet()) {
                keys[index] = bonus.getKey();
                weights[index] = bonus.getValue();
                index++;
            }

            ZParams minParams = new ZParams().aggregate(ZParams.Aggregate.MIN).weights(weights);
            String minimum = searchManager.zunion(trans, 30, minParams, keys);

            ZParams maxParams = new ZParams().aggregate(ZParams.Aggregate.MAX).weights(weights);
            String maximum = searchManager.zunion(trans, 30, maxParams, keys);

            String result = searchManager.zunion(
                    trans, 30, new ZParams().weights(2, 1, 1), base, minimum, maximum);
            return new Pair<Set<String>, String>(words, result);
        }
        return new Pair<Set<String>, String>(words, base);
    }

    public void recordTargetingResult(long targetId, String adId, Set<String> words) {
        Set<String> terms = client.smembers("terms:" + adId);
        String type = client.hget("type:", adId);

        Transaction trans = client.multi();
        terms.addAll(words);
        if (terms.size() > 0) {
            String matchedKey = "terms:matched:" + targetId;
            for (String term : terms) {
                trans.sadd(matchedKey, term);
            }
            trans.expire(matchedKey, 900);
        }

        trans.incr("type:" + type + ":views:");
        for (String term : terms) {
            trans.zincrby("views:" + adId, 1, term);
        }
        trans.zincrby("views:" + adId, 1, "");

        List<Object> response = trans.exec();
        double views = (Double) response.get(response.size() - 1);
        if ((views % 100) == 0) {
            updateCpms(adId);
        }
    }

    @SuppressWarnings("unchecked")
    public void updateCpms(String adId) {
        Transaction trans = client.multi();
        trans.hget("type:", adId);
        trans.zscore("ad:base_value:", adId);
        trans.smembers("terms:" + adId);
        List<Object> response = trans.exec();
        String type = (String) response.get(0);
        Double baseValue = (Double) response.get(1);
        Set<String> words = (Set<String>) response.get(2);

        String which = "clicks";
        Ecpm ecpm = Enum.valueOf(Ecpm.class, type.toUpperCase());
        if (Ecpm.CPA.equals(ecpm)) {
            which = "actions";
        }

        trans = client.multi();
        trans.get("type:" + type + ":views:");
        trans.get("type:" + type + ':' + which);
        response = trans.exec();
        String typeViews = (String) response.get(0);
        String typeClicks = (String) response.get(1);

        AVERAGE_PER_1K.put(ecpm,
                1000. *
                        Integer.valueOf(typeClicks != null ? typeClicks : "1") /
                        Integer.valueOf(typeViews != null ? typeViews : "1"));

        if (Ecpm.CPM.equals(ecpm)) {
            return;
        }

        String viewKey = "views:" + adId;
        String clickKey = which + ':' + adId;

        trans = client.multi();
        trans.zscore(viewKey, "");
        trans.zscore(clickKey, "");
        response = trans.exec();
        Double adViews = (Double) response.get(0);
        Double adClicks = (Double) response.get(1);

        double adEcpm = 0;
        if (adClicks == null || adClicks < 1) {
            Double score = client.zscore("idx:ad:value:", adId);
            adEcpm = score != null ? score.doubleValue() : 0;
        } else {
            adEcpm = toEcpm(
                    ecpm,
                    adViews != null ? adViews.doubleValue() : 1,
                    adClicks != null ? adClicks.doubleValue() : 0,
                    baseValue);
            client.zadd("idx:ad:value:", adEcpm, adId);
        }
        for (String word : words) {
            trans = client.multi();
            trans.zscore(viewKey, word);
            trans.zscore(clickKey, word);
            response = trans.exec();
            Double views = (Double) response.get(0);
            Double clicks = (Double) response.get(1);

            if (clicks == null || clicks < 1) {
                continue;
            }

            double wordEcpm = toEcpm(
                    ecpm,
                    views != null ? views.doubleValue() : 1,
                    clicks != null ? clicks.doubleValue() : 0,
                    baseValue);
            double bonus = wordEcpm - adEcpm;
            client.zadd("idx:" + word, bonus, adId);
        }
    }

    public void recordClick(long targetId, String adId, boolean action) {
        String type = client.hget("type:", adId);
        Ecpm ecpm = Enum.valueOf(Ecpm.class, type.toUpperCase());

        String clickKey = "clicks:" + adId;
        String matchKey = "terms:matched:" + targetId;
        Set<String> matched = client.smembers(matchKey);
        matched.add("");

        Transaction trans = client.multi();
        if (Ecpm.CPA.equals(ecpm)) {
            trans.expire(matchKey, 900);
            if (action) {
                clickKey = "actions:" + adId;
            }
        }

        if (action && Ecpm.CPA.equals(ecpm)) {
            trans.incr("type:" + type + ":actions:");
        } else {
            trans.incr("type:" + type + ":clicks:");
        }

        for (String word : matched) {
            trans.zincrby(clickKey, 1, word);
        }
        trans.exec();

        updateCpms(adId);
    }

}
