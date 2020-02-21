package org.lili.redis.inaction.chapter2;

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/2/20 23:49
 * @description
 * @notes
 */

public class WebManagerTest {

    private WebManager webManager;

    @Before
    public void setup() {
        webManager = new WebManager();
    }

    @Test
    public void checkToken() {
        String s = webManager.checkToken(UUID.randomUUID().toString());
        assertNull(s);
    }

    @Test
    public void updateToken() {
        String token = getToken();
        webManager.updateToken(token, "lili", "book1");
        webManager.updateToken(token, "lili", "book2");
        webManager.updateToken(token, "lili", "book3");
        webManager.updateToken(token, "lili", "book3");
        webManager.updateToken(token, "lili", "book3");
        webManager.updateToken(token, "lili", "book3");
        webManager.updateToken(token, "lili", "book3");
    }

    private String getToken() {
        return UUID.randomUUID().toString();
    }
}