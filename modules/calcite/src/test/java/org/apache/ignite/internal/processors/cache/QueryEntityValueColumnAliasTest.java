package org.apache.ignite.internal.processors.cache;

import java.sql.Date;
import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class QueryEntityValueColumnAliasTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final String KEY_COLUMN_NAME = "id";

    /** */
    private static final String VALUE_COLUMN_NAME = "value";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        SqlConfiguration sqlCfg = new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        cfg.setSqlConfiguration(sqlCfg);

        return cfg;
    }

    /** */
    @Test
    public void queryEntityValueColumnAliasTest() throws Exception {
        try (IgniteEx src = startGrid(0)) {
            QueryEntity qryEntity = new QueryEntity()
                .setTableName(CACHE_NAME)
                .setKeyFieldName(KEY_COLUMN_NAME)
                .setValueFieldName(VALUE_COLUMN_NAME)
                .addQueryField(KEY_COLUMN_NAME, Integer.class.getName(), null)
                .addQueryField(VALUE_COLUMN_NAME, Date.class.getName(), null);

            CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<Integer, Object>(qryEntity.getTableName())
                .setQueryEntities(Collections.singletonList(qryEntity));

            try (IgniteCache<Integer, Object> cache = src.getOrCreateCache(ccfg)) {
                long time = System.currentTimeMillis();

                Date date = new Date(time);

                cache.put(1, date);

                assert cache.get(1).equals(date);
            }
        }
    }
}
