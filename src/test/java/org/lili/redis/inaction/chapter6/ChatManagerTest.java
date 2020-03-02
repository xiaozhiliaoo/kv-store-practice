package org.lili.redis.inaction.chapter6;

import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 21:36
 * @description
 * @notes
 */

public class ChatManagerTest {

    private ChatManager chatManager;

    @Before
    public void setUp() throws Exception {
        chatManager = new ChatManager();
    }

    @Test
    public void createChat() {
        Set<String> recipients = new HashSet<String>();
        recipients.add("jeff");
        recipients.add("jenny");
        String chatId = chatManager.createChat("joe", recipients, "message 1");
        System.out.println("Now let's send a few messages...");
        for (int i = 2; i < 5; i++) {
            chatManager.sendMessage(chatId, "joe", "message " + i);
        }
    }
}