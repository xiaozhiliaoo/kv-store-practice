package org.lili.redis.inaction.chapter7;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lili
 * @date 2020/3/2 22:23
 * @description 搜索
 * @notes
 */

public class SearchManager extends Base {
    private static final Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}");
    private static final Pattern WORDS_RE = Pattern.compile("[a-z']{2,}");
    private static final Set<String> STOP_WORDS = new HashSet<String>();

    static {
        for (String word :
                ("able about across after all almost also am among " +
                        "an and any are as at be because been but by can " +
                        "cannot could dear did do does either else ever " +
                        "every for from get got had has have he her hers " +
                        "him his how however if in into is it its just " +
                        "least let like likely may me might most must my " +
                        "neither no nor not of off often on only or other " +
                        "our own rather said say says she should since so " +
                        "some than that the their them then there these " +
                        "they this tis to too twas us wants was we were " +
                        "what when where which while who whom why will " +
                        "with would yet you your").split(" ")) {
            STOP_WORDS.add(word);
        }
    }


    private static String CONTENT = "this is some random content, look at how it is indexed.";

    public Set<String> tokenize(String content) {
        Set<String> words = new HashSet<>();
        Matcher matcher = WORDS_RE.matcher(content);
        while (matcher.find()) {
            String word = matcher.group().trim();
            if (word.length() > 2 && !STOP_WORDS.contains(word)) {
                words.add(word);
            }
        }
        return words;
    }

    public int indexDocument(String docid, String content) {
        Set<String> words = tokenize(content);
        Transaction trans = client.multi();
        for (String word : words) {
            trans.sadd("idx:" + word, docid);
        }
        return trans.exec().size();
    }

    private String setCommon(Transaction trans, String method, int ttl, String... items) {
        String[] keys = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            keys[i] = "idx:" + items[i];
        }
        String id = UUID.randomUUID().toString();
        try {
            trans.getClass()
                    .getDeclaredMethod(method, String.class, String[].class)
                    .invoke(trans, "idx:" + id, keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        trans.expire("idx:" + id, ttl);
        return id;
    }

    public String intersect(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sinterstore", ttl, items);
    }

    public String union(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sunionstore", ttl, items);
    }

    public String difference(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sdiffstore", ttl, items);
    }

    private String zsetCommon(Transaction trans, String method, int ttl, ZParams params, String... sets) {
        String[] keys = new String[sets.length];
        for (int i = 0; i < sets.length; i++) {
            keys[i] = "idx:" + sets[i];
        }

        String id = UUID.randomUUID().toString();
        try {
            trans.getClass()
                    .getDeclaredMethod(method, String.class, ZParams.class, String[].class)
                    .invoke(trans, "idx:" + id, params, keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        trans.expire("idx:" + id, ttl);
        return id;
    }

    public String zintersect(Transaction trans, int ttl, ZParams params, String... sets) {
        return zsetCommon(trans, "zinterstore", ttl, params, sets);
    }

    public String zunion(Transaction trans, int ttl, ZParams params, String... sets) {
        return zsetCommon(trans, "zunionstore", ttl, params, sets);
    }

    public class Query {
        public final List<List<String>> all = new ArrayList<List<String>>();
        public final Set<String> unwanted = new HashSet<String>();
    }

    public Query parse(String queryString) {
        Query query = new Query();
        Set<String> current = new HashSet<String>();
        Matcher matcher = QUERY_RE.matcher(queryString.toLowerCase());
        while (matcher.find()) {
            String word = matcher.group().trim();
            char prefix = word.charAt(0);
            if (prefix == '+' || prefix == '-') {
                word = word.substring(1);
            }

            if (word.length() < 2 || STOP_WORDS.contains(word)) {
                continue;
            }

            if (prefix == '-') {
                query.unwanted.add(word);
                continue;
            }

            if (!current.isEmpty() && prefix != '+') {
                query.all.add(new ArrayList<String>(current));
                current.clear();
            }
            current.add(word);
        }

        if (!current.isEmpty()) {
            query.all.add(new ArrayList<String>(current));
        }
        return query;
    }


    public String parseAndSearch(String queryString, int ttl) {
        Query query = parse(queryString);
        if (query.all.isEmpty()) {
            return null;
        }

        List<String> toIntersect = new ArrayList<String>();
        for (List<String> syn : query.all) {
            if (syn.size() > 1) {
                Transaction trans = client.multi();
                toIntersect.add(union(trans, ttl, syn.toArray(new String[syn.size()])));
                trans.exec();
            } else {
                toIntersect.add(syn.get(0));
            }
        }

        String intersectResult = null;
        if (toIntersect.size() > 1) {
            Transaction trans = client.multi();
            intersectResult = intersect(
                    trans, ttl, toIntersect.toArray(new String[toIntersect.size()]));
            trans.exec();
        } else {
            intersectResult = toIntersect.get(0);
        }

        if (!query.unwanted.isEmpty()) {
            String[] keys = query.unwanted
                    .toArray(new String[query.unwanted.size() + 1]);
            keys[keys.length - 1] = intersectResult;
            Transaction trans = client.multi();
            intersectResult = difference(trans, ttl, keys);
            trans.exec();
        }

        return intersectResult;
    }

    @SuppressWarnings("unchecked")
    public SearchResult searchAndSort(String queryString, String sort) {
        boolean desc = sort.startsWith("-");
        if (desc) {
            sort = sort.substring(1);
        }
        boolean alpha = !"updated".equals(sort) && !"id".equals(sort);
        String by = "kb:doc:*->" + sort;

        String id = parseAndSearch(queryString, 300);

        Transaction trans = client.multi();
        trans.scard("idx:" + id);
        SortingParams params = new SortingParams();
        if (desc) {
            params.desc();
        }
        if (alpha) {
            params.alpha();
        }
        params.by(by);
        params.limit(0, 20);
        trans.sort("idx:" + id, params);
        List<Object> results = trans.exec();

        return new SearchResult(
                id,
                ((Long) results.get(0)).longValue(),
                (List<String>) results.get(1));
    }

    public class SearchResult {
        public final String id;
        public final long total;
        public final List<String> results;

        public SearchResult(String id, long total, List<String> results) {
            this.id = id;
            this.total = total;
            this.results = results;
        }
    }

    /**
     * @param queryString
     * @param desc
     * @param weights     查询权重
     * @return
     */
    @SuppressWarnings("unchecked")
    public SearchResult searchAndZsort(String queryString, boolean desc, Map<String, Integer> weights) {
        int ttl = 300;
        int start = 0;
        int num = 20;
        String id = parseAndSearch(queryString, ttl);

        //文章更新时间
        int updateWeight = weights.containsKey("update") ? weights.get("update") : 1;
        //文章投票数字
        int voteWeight = weights.containsKey("vote") ? weights.get("vote") : 0;

        String[] keys = new String[]{id, "sort:update", "sort:votes"};
        Transaction trans = client.multi();
        id = zintersect(trans, ttl, new ZParams().weights(0, updateWeight, voteWeight), keys);

        trans.zcard("idx:" + id);
        if (desc) {
            trans.zrevrange("idx:" + id, start, start + num - 1);
        } else {
            trans.zrange("idx:" + id, start, start + num - 1);
        }
        List<Object> results = trans.exec();

        return new SearchResult(
                id,
                ((Long) results.get(results.size() - 2)).longValue(),
                // Note: it's a LinkedHashSet, so it's ordered
                new ArrayList<String>((Set<String>) results.get(results.size() - 1)));
    }

    public long stringToScore(String string) {
        return stringToScore(string, false);
    }

    public long stringToScore(String string, boolean ignoreCase) {
        if (ignoreCase) {
            string = string.toLowerCase();
        }

        List<Integer> pieces = new ArrayList<Integer>();
        for (int i = 0; i < Math.min(string.length(), 6); i++) {
            pieces.add((int) string.charAt(i));
        }
        while (pieces.size() < 6) {
            pieces.add(-1);
        }

        long score = 0;
        for (int piece : pieces) {
            score = score * 257 + piece + 1;
        }

        return score * 2 + (string.length() > 6 ? 1 : 0);
    }

    public long stringToScoreGeneric(String string, Map<Integer, Integer> mapping) {
        int length = (int) (52 / (Math.log(mapping.size()) / Math.log(2)));

        List<Integer> pieces = new ArrayList<Integer>();
        for (int i = 0; i < Math.min(string.length(), length); i++) {
            pieces.add((int) string.charAt(i));
        }
        while (pieces.size() < 6) {
            pieces.add(-1);
        }

        long score = 0;
        for (int piece : pieces) {
            int value = mapping.get(piece);
            score = score * mapping.size() + value + 1;
        }

        return score * 2 + (string.length() > 6 ? 1 : 0);
    }


}
