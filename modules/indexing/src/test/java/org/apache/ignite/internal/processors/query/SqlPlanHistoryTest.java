package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class SqlPlanHistoryTest extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new IndexingQueryEngineConfiguration()));
    }

    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrids(2);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>("test")
                .setQueryEntities(Collections.singleton(
                    new QueryEntity()
                        .setTableName("test")
                        .setKeyType(Integer.class.getName())
                        .setValueType(Integer.class.getName()))));

        for (int i = 0; i < 5; i++)
            cache.put(i, i);

        Iterator<List<?>> iter1 = cache.query(
            new SqlFieldsQuery("SELECT * FROM test").setLocal(false)).iterator();

        assertTrue(iter1.hasNext());
    }
}
