package org.lili.redis.inaction.chapter1;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/2/13 1:17
 * @description
 * @notes
 */

public class VoteManagerTest {

    private String article(Long articleId) {
        return "article:" + articleId;
    }

    @Test
    public void postArticle() {
        String user = "lili";
        VoteManager voteManager = new VoteManager();
        long articleId = voteManager.postArticle(user,
                "doHomework", "www.baidu.com");
        System.out.println(articleId);

        voteManager.articleVote(user, article(articleId));
    }
}