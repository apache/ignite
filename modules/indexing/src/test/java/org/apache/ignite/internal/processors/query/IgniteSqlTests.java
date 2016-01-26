package org.apache.ignite.internal.processors.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import java.util.List;

public class IgniteSqlTests extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setIgniteHome("c:\\w\\incubator-ignite");

        cfg.setCacheConfiguration(new CacheConfiguration("sqlCache").setIndexedTypes(QueryPerson.class));

        return cfg;
    }

    public void testQueries() throws Exception {
        Ignite i = startGrid();

        IgniteCache<Integer, QueryPerson> c = i.getOrCreateCache("sqlCache");

        QueryPerson p = new QueryPerson();
        p.Age = 20;
        p.Name = "vasya";

        c.put(1, p);

        List<Cache.Entry<Integer, QueryPerson>> results =
            c.query(new SqlQuery<Integer, QueryPerson>("QueryPerson", "Age > 10")).getAll();

        assert results.size() == 1;

        stopAllGrids();
    }

    public static class QueryPerson {
        @QuerySqlField(index = false)
        public Integer Age;

        @QuerySqlField(index = false)
        public String Name;
    }
}
