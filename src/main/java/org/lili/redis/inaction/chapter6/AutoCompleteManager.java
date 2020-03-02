package org.lili.redis.inaction.chapter6;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

/**
 * @author lili
 * @date 2020/3/1 17:04
 * @description
 * @notes
 */

public class AutoCompleteManager extends Base {

    public void addUpdateContact(String user, String contact) {
        String acList = "recent:" + user;
        Transaction trans = client.multi();
        trans.lrem(acList, 0, contact);
        trans.lpush(acList, contact);
        trans.ltrim(acList, 0, 99);
        trans.exec();
    }

    public void removeContact(String user, String contact) {
        client.lrem("recent:" + user, 0, contact);
    }

    public List<String> fetchAutocompleteList(String user, String prefix) {
        List<String> candidates = client.lrange("recent:" + user, 0, -1);
        List<String> matches = new ArrayList<String>();
        for (String candidate : candidates) {
            if (candidate.toLowerCase().startsWith(prefix)) {
                matches.add(candidate);
            }
        }
        return matches;
    }


    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";

    public String[] findPrefixRange(String prefix) {
        int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        String start = prefix.substring(0, prefix.length() - 1) + suffix + '{';
        String end = prefix + '{';
        return new String[]{start, end};
    }

    public void joinGuild(String guild, String user) {
        client.zadd("members:" + guild, 0, user);
    }

    public void leaveGuild(String guild, String user) {
        client.zrem("members:" + guild, user);
    }

    /**
     * @param guild  公会名字
     * @param prefix 前缀
     * @return 公会玩家
     */
    @SuppressWarnings("unchecked")
    public Set<String> autocompleteOnPrefix(String guild, String prefix) {
        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];
        String identifier = UUID.randomUUID().toString();
        start += identifier;
        end += identifier;
        String zsetName = "members:" + guild;

        client.zadd(zsetName, 0, start);
        client.zadd(zsetName, 0, end);

        Set<String> items = null;
        while (true) {
            client.watch(zsetName);
            int sindex = client.zrank(zsetName, start).intValue();
            int eindex = client.zrank(zsetName, end).intValue();
            int erange = Math.min(sindex + 9, eindex - 2);

            Transaction trans = client.multi();
            trans.zrem(zsetName, start);
            trans.zrem(zsetName, end);
            trans.zrange(zsetName, sindex, erange);
            List<Object> results = trans.exec();
            if (results != null) {
                items = (Set<String>) results.get(results.size() - 1);
                break;
            }
        }

        for (Iterator<String> iterator = items.iterator(); iterator.hasNext(); ) {
            if (iterator.next().indexOf('{') != -1) {
                iterator.remove();
            }
        }
        return items;
    }


}
