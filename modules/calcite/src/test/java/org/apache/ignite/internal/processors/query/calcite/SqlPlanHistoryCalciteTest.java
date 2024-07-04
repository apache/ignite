package org.apache.ignite.internal.processors.query.calcite;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class SqlPlanHistoryCalciteTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrid(1);

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

//        Iterator<Cache.Entry<Object, Object>> iter2 = cache.query(
//            new ScanQuery<>()).iterator();

//        Iterator<Cache.Entry<Object, Object>> iter3 = cache.query(
//            new IndexQuery<>(Integer.class)).iterator();

        assertTrue(iter1.hasNext());
//        assertTrue(iter2.hasNext());
//        assertTrue(iter3.hasNext());
    }
}
