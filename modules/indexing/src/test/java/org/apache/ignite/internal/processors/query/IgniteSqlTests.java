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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setCacheConfiguration(new CacheConfiguration("sqlCache")
                .setIndexedTypes(Integer.class, QueryPerson.class));
    }

    public void testSimpleQuery() throws Exception {
        IgniteCache<Integer, QueryPerson> c = getCache();

        List<Cache.Entry<Integer, QueryPerson>> results =
            c.query(new SqlQuery<Integer, QueryPerson>("QueryPerson", "Age > 10")).getAll();

        assert results.size() == 1;
    }

    public void testKeyQuery() throws Exception {
        IgniteCache<Integer, QueryPerson> c = getCache();

        List<Cache.Entry<Integer, QueryPerson>> results =
            c.query(new SqlQuery<Integer, QueryPerson>("QueryPerson", "_key < 10")).getAll();

        assert results.size() == 1;
    }

    private IgniteCache<Integer, QueryPerson> getCache() throws Exception {
        Ignite i = startGrid();

        IgniteCache<Integer, QueryPerson> c = i.cache("sqlCache");

        QueryPerson p = new QueryPerson();
        p.Age = 20;
        p.Name = "vasya";

        c.put(1, p);
        return c;
    }

    public static class QueryPerson {
        @QuerySqlField(index = false)
        public Integer Age;

        @QuerySqlField(index = false)
        public String Name;
    }
}
