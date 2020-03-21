package org.lili.redis.inaction.chapter7;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author lili
 * @date 2020/3/2 22:43
 * @description
 * @notes
 */

public class SearchManagerTest {

    private SearchManager searchManager;

    private static String CONTENT = "this is some random content, look at how it is indexed.";
    private static String CONTENT2 = "Thank you, all of my reviewers, for the first, second, and third reviews of my manuscript during development, and to my final QA reviewer. I tried to take all of your advice into consideration whenever I could. Thanks to Amit Nandi, Bennett Andrews, Bobby Abraham, Brian Forester, Brian Gyss, Brian McNamara, Daniel Sundman, David Miller, Felipe Gutierrez, Filippo Pacini, Gerard O’ Sullivan, JC Pretorius, Jonathan Crawley, Joshua White, Leo Cassarani, Mark Wigmans, Richard Clayton, Scott Lyons, Thomas O’Rourke, and Todd Fiala";
    private static String CONTENT3 = "I also thank my development editor Bert Bates: thank you for pointing out that my writing style needed to change for a book audience. Your influence on my writing early on continued during my work on the entire book, which I hope is reasonably accessible to readers with a wide range of expertise.";

    @Before
    public void setUp() throws Exception {
        searchManager = new SearchManager();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void tokenize() {
        Set<String> tokenize = searchManager.tokenize(CONTENT);
        assertNotNull(tokenize);
    }

    @Test
    public void indexDocument() {
        searchManager.indexDocument("doc1", CONTENT);
        searchManager.indexDocument("doc2", CONTENT2);
        searchManager.indexDocument("doc3", CONTENT3);
    }

    @Test
    public void parse() {
        SearchManager.Query parse = searchManager.parse("connect +connection +disconnect +disconnection chat -proxy -proxies");
        assertNotNull(parse);
    }

}