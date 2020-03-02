package org.lili.redis.inaction.chapter6;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.lili.redis.inaction.base.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.*;

/**
 * @author lili
 * @date 2020/3/1 19:30
 * @description
 * @notes
 */

public class ChatManager extends Base {

    public String createChat(String sender, Set<String> recipients, String message) {
        //全局唯一的chatId
        String chatId = String.valueOf(client.incr("ids:chat:"));
        return createChat(sender, recipients, message, chatId);
    }

    /**
     * @param sender     发送者
     * @param recipients 接收者
     * @param message    消息
     * @param chatId     群组id
     * @return
     */
    public String createChat(String sender, Set<String> recipients, String message, String chatId) {
        recipients.add(sender);

        Transaction trans = client.multi();
        for (String recipient : recipients) {
            trans.zadd("chat:" + chatId, 0, recipient);
            trans.zadd("seen:" + recipient, 0, chatId);
        }
        trans.exec();

        return sendMessage(chatId, sender, message);
    }

    public String sendMessage(String chatId, String sender, String message) {
        DistributedLock lock = new DistributedLock();
        String identifier = lock.acquireLock("chat:" + chatId);
        if (identifier == null) {
            throw new RuntimeException("Couldn't get the lock");
        }
        try {
            long messageId = client.incr("ids:" + chatId);
            HashMap<String, Object> values = new HashMap<String, Object>();
            values.put("id", messageId);
            values.put("ts", System.currentTimeMillis());
            values.put("sender", sender);
            values.put("message", message);
            String packed = new Gson().toJson(values);
            //消息id作为分手
            client.zadd("msgs:" + chatId, messageId, packed);
        } finally {
            lock.releaseLock("chat:" + chatId, identifier);
        }
        return chatId;
    }

    public class ChatMessages {
        public String chatId;
        public List<Map<String, Object>> messages;

        public ChatMessages(String chatId, List<Map<String, Object>> messages) {
            this.chatId = chatId;
            this.messages = messages;
        }

        public boolean equals(Object other) {
            if (!(other instanceof ChatMessages)) {
                return false;
            }
            ChatMessages otherCm = (ChatMessages) other;
            return chatId.equals(otherCm.chatId) && messages.equals(otherCm.messages);
        }
    }


    /**
     * @param recipient 接受者
     * @return
     */
    public List<ChatMessages> fetchPendingMessages(String recipient) {
        //最后接收消息
        Set<Tuple> seenSet = client.zrangeWithScores("seen:" + recipient, 0, -1);
        List<Tuple> seenList = new ArrayList<Tuple>(seenSet);

        Transaction trans = client.multi();
        for (Tuple tuple : seenList) {
            //群组
            String chatId = tuple.getElement();
            //已读最大消息id
            int seenId = (int) tuple.getScore();
            //min:seenId + 1  max:inf  获取用户所有群组未读消息
            trans.zrangeByScore("msgs:" + chatId, String.valueOf(seenId + 1), "inf");
        }

        List<Object> results = trans.exec();

        Gson gson = new Gson();
        Iterator<Tuple> seenIterator = seenList.iterator();
        Iterator<Object> resultsIterator = results.iterator();

        List<ChatMessages> chatMessages = new ArrayList<>();
        List<Object[]> seenUpdates = new ArrayList<>();
        List<Object[]> msgRemoves = new ArrayList<>();
        //获取未读消息：遍历所有参与群组，获取群组未读消息，清理被所有用户看过的消息
        while (seenIterator.hasNext()) {
            Tuple seen = seenIterator.next();
            Set<String> messageStrings = (Set<String>) resultsIterator.next();
            if (messageStrings.size() == 0) {
                continue;
            }

            int seenId = 0;
            String chatId = seen.getElement();
            List<Map<String, Object>> messages = new ArrayList<>();
            for (String messageJson : messageStrings) {
                Map<String, Object> message =  gson.fromJson(
                        messageJson, new TypeToken<Map<String, Object>>() {
                        }.getType());
                int messageId = ((Double) message.get("id")).intValue();
                if (messageId > seenId) {
                    seenId = messageId;
                }
                message.put("id", messageId);
                messages.add(message);
            }
            //更新已读消息有序集合
            client.zadd("chat:" + chatId, seenId, recipient);
            seenUpdates.add(new Object[]{"seen:" + recipient, seenId, chatId});

            //所有人已读消息
            Set<Tuple> minIdSet = client.zrangeWithScores("chat:" + chatId, 0, 0);
            if (minIdSet.size() > 0) {
                msgRemoves.add(new Object[]{"msgs:" + chatId, minIdSet.iterator().next().getScore()});
            }
            chatMessages.add(new ChatMessages(chatId, messages));
        }

        trans = client.multi();
        for (Object[] seenUpdate : seenUpdates) {
            trans.zadd((String) seenUpdate[0], (Integer) seenUpdate[1], (String) seenUpdate[2]);
        }
        for (Object[] msgRemove : msgRemoves) {
            trans.zremrangeByScore((String) msgRemove[0], 0, ((Double) msgRemove[1]).intValue());
        }
        trans.exec();
        return chatMessages;
    }

    public void joinChat(String chatId, String user) {
        long messageId = client.incr("ids:" + chatId);
        Transaction trans = client.multi();
        trans.zadd("chat:" + chatId, messageId, user);
        trans.zadd("seen:" + user, messageId, chatId);
        trans.exec();
    }

    public void leaveChat(String chatId, String user) {
        Transaction transaction = client.multi();
        transaction.zrem("chat:" + chatId, user);
        transaction.zrem("seen:" + user, chatId);
        transaction.zcard("chat:"+chatId);
        List<Object> results = transaction.exec();

    }


}
