package org.apache.ignite;

import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class LimitTest extends GridCommonAbstractTest {

    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryEntity qe = new QueryEntity(Long.class.getName(), Value.class.getName())
            .setTableName("VALCACHE")
            .setKeyFieldName("id")
            .setValueFieldName("name")
            .setFields(
                new LinkedHashMap<>(F.asMap("id", Long.class.getName(), "name", String.class.getName())));

        CacheConfiguration ccfg = new CacheConfiguration()
            .setName("CACHE")
            .setQueryEntities(F.asList(qe));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        Ignite node = startGrids(2);

        int limit = 3000;

        IgniteCache cache = node.cache("CACHE");

        for (int i = 0; i < 15_000; i++)
            cache.put((long) i, new Value(i, String.valueOf(i)));

        List<List<?>> result = cache
            .query(new SqlFieldsQuery("select * from VALCACHE limit ?").setArgs(limit))
            .getAll();

        assertEquals(limit, result.size());
    }

    static class Value {
        private long id;
        private String name;

        Value(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
