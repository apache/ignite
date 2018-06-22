package org.apache.ignite.transactions.spring;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

public class GridSpringTransactionManagerSpringBeanSelfTest extends GridSpringTransactionManagerAbstractTest {

    /** */
    private Ignite ignite;

    /** */
    private GridSpringTransactionService service;

    @Override public IgniteCache<Integer, String> cache() {
        return ignite.cache(CACHE_NAME);
    }

    @Override public GridSpringTransactionService service() {
        return service;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ApplicationContext appCtx = new GenericXmlApplicationContext("config/spring-transactions-ignite-spring-bean.xml");
        ignite = (Ignite)appCtx.getBean("mySpringBean");
        service = (GridSpringTransactionService)appCtx.getBean("gridSpringTransactionService");
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }
}
