package org.lili.redis.inaction.chapter6;

import com.alibaba.fastjson.JSON;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/1 17:11
 * @description
 * @notes
 */

public class AutoCompleteManagerTest {

    private AutoCompleteManager autoCompleteManager;

    @Before
    public void setUp() throws Exception {
        autoCompleteManager = new AutoCompleteManager();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void addUpdateContact() {
        for (int i = 0; i < 10; i++) {
            autoCompleteManager.addUpdateContact("user", "contact-" + ((int) Math.floor(i / 3)) + '-' + i);
        }

        autoCompleteManager.removeContact("user", "contact-2-6");

        List<String> contacts = autoCompleteManager.fetchAutocompleteList("user", "c");
        System.out.println(JSON.toJSONString(contacts));

        contacts = autoCompleteManager.fetchAutocompleteList("user", "contact-2-");
        System.out.println(JSON.toJSONString(contacts));

    }

    @Test
    public void autocompleteOnPrefix() {
        for (String name : new String[]{"jeff", "jenny", "jack", "jennifer"}) {
            autoCompleteManager.joinGuild("test", name);
        }

        Set<String> r = autoCompleteManager.autocompleteOnPrefix("test", "je");

        System.out.println(JSON.toJSONString(r));

        autoCompleteManager.leaveGuild("test", "jeff");
        r = autoCompleteManager.autocompleteOnPrefix("test", "je");

        System.out.println(JSON.toJSONString(r));

    }

    @Test
    public void findPrefixRange() {
        String[] jes = autoCompleteManager.findPrefixRange("je");
        System.out.println(jes[0]); //jd{
        System.out.println(jes[1]); //je{
    }
}