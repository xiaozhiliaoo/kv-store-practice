package org.lili.redis.inaction.chapter5;

import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * @author lili
 * @date 2020/3/1 14:08
 * @description
 * @notes
 */

public class LogManager extends Base {

    public static final String DEBUG = "debug";
    public static final String INFO = "info";
    public static final String WARNING = "warning";
    public static final String ERROR = "error";
    public static final String CRITICAL = "critical";

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("EEE MMM dd HH:00:00 yyyy");

    private static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");

    static {
//        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTF+8"));
    }


    public void logRecent(String name, String message) {
        logRecent(name, message, INFO);
    }

    public void logRecent(String name, String message, String severity) {
        String destination = "recent:" + name + ':' + severity;
        Pipeline pipe = client.pipelined();
        pipe.lpush(destination, TIMESTAMP.format(new Date()) + ' ' + message);
        pipe.ltrim(destination, 0, 99);
        pipe.sync();
    }

    public void logCommon(String name, String message) {
        logCommon(name, message, INFO, 5000);
    }

    public void logCommon(String name, String message, String severity, int timeout) {
        String commonDest = "common:" + name + ':' + severity;
        //当前所处小时数
        String startKey = commonDest + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end){
            client.watch(startKey);
            //开始rolling时间
            String hourStart = TIMESTAMP.format(new Date());
//            String hourStart = ISO_FORMAT.format(new Date());
            String existing = client.get(startKey);

            Transaction trans = client.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0){
                trans.rename(commonDest, commonDest + ":last");
                trans.rename(startKey, commonDest + ":pstart");
                trans.set(startKey, hourStart);
            }

            trans.zincrby(commonDest, 1, message);

            String recentDest = "recent:" + name + ':' + severity;
            trans.lpush(recentDest, TIMESTAMP.format(new Date()) + ' ' + message);
            trans.ltrim(recentDest, 0, 99);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null){
                continue;
            }
            return;
        }
    }


}
