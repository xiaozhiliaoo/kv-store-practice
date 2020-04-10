package org.lili.redis.base;

/**
 * @author lili
 * @date 2020/4/6 13:15
 * @description 用户上线次数统计，在线天数统计
 * @notes
 */

import org.lili.redis.inaction.base.Base;

import java.util.*;

public class UserOnlineDayCount extends Base {

    /**
     * BITCOUNT peter
     * @param userId
     * @return 上线天数
     */
    public long count(int userId) {
        return client.bitcount(String.valueOf(userId));
    }


    /**
     * SETBIT peter 101 1
     * SETBIT peter 102 1
     * SETBIT peter 103 1
     * @param userId
     */
    public void access(int userId) {
        //公司成立天数
        //int companyDay = System.currentTimeMillis() - "20181005";
        int companyDay = 0;
        client.setbit(String.valueOf(userId), companyDay, true);
    }


}
