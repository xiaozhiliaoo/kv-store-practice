package org.lili.redis.inaction.chapter3;

import org.junit.After;
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

    @Test
    public void byteString() {
        dataTypeManager.byteString();
    }

    @Test
    public void list() {
        dataTypeManager.list();
    }

    @Test
    public void blist() {
        dataTypeManager.blist();
    }

    @Test
    public void set() {
        dataTypeManager.set();
    }

    @Test
    public void mathSet() {
        dataTypeManager.mathSet();
    }

    @Test
    public void hash() {
        dataTypeManager.hash();
    }

    @After
    public void after() {

    }

    @Test
    public void zset() {
        dataTypeManager.zset();
    }

    @Test
    public void zsetMuitl() {
        dataTypeManager.zsetMuitl();
    }
}