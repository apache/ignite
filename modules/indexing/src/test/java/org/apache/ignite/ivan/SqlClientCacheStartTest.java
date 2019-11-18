package org.apache.ignite.ivan;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class SqlClientCacheStartTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSystemWorkerBlockedTimeout(Long.MAX_VALUE);
    }

    @Test
    public void test() throws Exception {
        IgniteEx g0 = startGrid(0);
        g0.context().query().querySqlFields(new SqlFieldsQuery("create table Person(id int primary key, name varchar)"), false);

        G.setClientMode(true);

        IgniteEx cli = startGrid(42);
        IgniteCache<Object, Object> cache = cli.createCache(new CacheConfiguration<>("dummy"));
        cli.cache("SQL_PUBLIC_PERSON").close();
        System.err.println(cache.query(new SqlFieldsQuery("select * from Person")).getAll());
    }
}
