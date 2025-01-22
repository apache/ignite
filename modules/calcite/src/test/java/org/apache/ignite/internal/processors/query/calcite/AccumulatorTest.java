package org.apache.ignite.internal.processors.query.calcite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class AccumulatorTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        try (Ignite ign = startGrid()) {
            IgniteCache<?, ?> cache = ign.createCache(DEFAULT_CACHE_NAME);

            cache.query(new SqlFieldsQuery("create table my_table(id int primary key, age int);"));

            cache.query(new SqlFieldsQuery("insert into my_table values (0, 0)"));

            System.out.println("SELECT");

            Object res = cache.query(new SqlFieldsQuery("select age, sum(age), count(age) from my_table group by age;")).getAll();

            System.out.println(res);
        }
    }
}
