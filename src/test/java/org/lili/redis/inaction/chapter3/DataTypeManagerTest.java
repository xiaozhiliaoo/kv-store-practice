package org.lili.redis.inaction.chapter3;

import org.junit.Before;
import org.junit.Test;
import org.lili.redis.inaction.chapter2.WebManager;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/2/21 0:12
 * @description
 * @notes
 */

public class DataTypeManagerTest {

    private DataTypeManager dataTypeManager;

    @Before
    public void setup() {
        dataTypeManager = new DataTypeManager();
    }

    @Test
    public void string() {
        dataTypeManager.string();
    }
}