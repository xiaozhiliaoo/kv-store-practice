package org.lili.redis.inaction.chapter4;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 12:04
 * @description
 * @notes
 */

public class MarketManagerTest {

    private MarketManager marketManager;

    @Before
    public void setup() {
        marketManager = new MarketManager();
        marketManager.createUser();
        marketManager.createInventory();
    }

    @Test
    public void listItem() {
        //用户17将ItemM以97价格出售
        marketManager.listItem("ItemM","17",97);

        //用户27以97块在市场U买17出售的ItemM
        marketManager.purchaseItem("27","ItemM","17",97d);
    }

    @Test
    public void purchaseItem() {
    }


    @Test
    public void benchmarkUpdateToken() {
        marketManager.benchmarkUpdateToken(1);
    }
}