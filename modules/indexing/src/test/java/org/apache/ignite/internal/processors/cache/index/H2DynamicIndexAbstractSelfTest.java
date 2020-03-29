/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Test that checks indexes handling on H2 side.
 */
public abstract class H2DynamicIndexAbstractSelfTest extends AbstractSchemaSelfTest {
    /** Client node index. */
    private static final int CLIENT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        createSqlCache(client(), cacheConfiguration());

        assertNoIndex(CACHE_NAME, TBL_NAME_ESCAPED, IDX_NAME_1_ESCAPED);

        IgniteCache<KeyClass, ValueClass> cache = client().cache(CACHE_NAME);

        cache.put(new KeyClass(1), new ValueClass(1L));
        cache.put(new KeyClass(2), new ValueClass(2L));
        cache.put(new KeyClass(3), new ValueClass(3L));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        destroySqlCache(client());

        super.afterTest();
    }

    /**
     * Test that after index creation index is used by queries.
     */
    @Test
    public void testCreateIndex() throws Exception {
        IgniteCache<KeyClass, ValueClass> cache = cache();

        assertSize(3);

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)")).getAll();

        // Test that local queries on all nodes use new index.
        for (int i = 0; i < 4; i++) {
            if (ignite(i).configuration().isClientMode())
                continue;

            List<List<?>> locRes = ignite(i).cache("cache").query(new SqlFieldsQuery("explain select \"id\" from " +
                "\"cache\".\"ValueClass\" where \"field1\" = 1").setLocal(true)).getAll();

            assertEquals(F.asList(
                Collections.singletonList("SELECT\n" +
                    "    \"id\"\n" +
                    "FROM \"cache\".\"ValueClass\"\n" +
                    "    /* \"cache\".\"idx_1\": \"field1\" = 1 */\n" +
                    "WHERE \"field1\" = 1")
            ), locRes);
        }

        assertSize(3);

        cache.remove(new KeyClass(2));

        assertSize(2);

        cache.put(new KeyClass(4), new ValueClass(1L));

        assertSize(3);
    }

    /**
     * Test that creating an index with duplicate name yields an error.
     */
    @Test
    public void testCreateIndexWithDuplicateName() {
        final IgniteCache<KeyClass, ValueClass> cache = cache();

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)"));

        assertSqlException(new Runnable() {
            @Override public void run() {
                cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" +
                    TBL_NAME_ESCAPED + "\"(\"id\" ASC)"));
            }
        }, IgniteQueryErrorCode.INDEX_ALREADY_EXISTS);
    }

    /**
     * Test that creating an index with duplicate name does not yield an error with {@code IF NOT EXISTS}.
     */
    @Test
    public void testCreateIndexIfNotExists() {
        final IgniteCache<KeyClass, ValueClass> cache = cache();

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)"));

        cache.query(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS \"" + IDX_NAME_1_ESCAPED + "\" ON \"" +
            TBL_NAME_ESCAPED + "\"(\"id\" ASC)"));
    }

    /**
     * Test that after index drop there are no attempts to use it, and data state remains intact.
     */
    @Test
    public void testDropIndex() {
        IgniteCache<KeyClass, ValueClass> cache = cache();

        assertSize(3);

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)"));

        assertSize(3);

        cache.query(new SqlFieldsQuery("DROP INDEX \"" + IDX_NAME_1_ESCAPED + "\""));

        // Test that no local queries on all nodes use new index.
        for (int i = 0; i < 4; i++) {
            if (ignite(i).configuration().isClientMode())
                continue;

            List<List<?>> locRes = ignite(i).cache("cache").query(new SqlFieldsQuery("explain select \"id\" from " +
                "\"cache\".\"ValueClass\" where \"field1\" = 1").setLocal(true)).getAll();

            assertEquals(F.asList(
                Collections.singletonList("SELECT\n" +
                    "    \"id\"\n" +
                    "FROM \"cache\".\"ValueClass\"\n" +
                    "    /* \"cache\".\"ValueClass\".__SCAN_ */\n" +
                    "WHERE \"field1\" = 1")
            ), locRes);
        }

        assertSize(3);
    }

    /**
     * Test that dropping a non-existent index yields an error.
     */
    @Test
    public void testDropMissingIndex() {
        final IgniteCache<KeyClass, ValueClass> cache = cache();

        assertSqlException(new Runnable() {
            @Override public void run() {
                cache.query(new SqlFieldsQuery("DROP INDEX \"" + IDX_NAME_1_ESCAPED + "\""));
            }
        }, IgniteQueryErrorCode.INDEX_NOT_FOUND);
    }

    /**
     * Test that dropping a non-existent index does not yield an error with {@code IF EXISTS}.
     */
    @Test
    public void testDropMissingIndexIfExists() {
        final IgniteCache<KeyClass, ValueClass> cache = cache();

        cache.query(new SqlFieldsQuery("DROP INDEX IF EXISTS \"" + IDX_NAME_1_ESCAPED + "\""));
    }

    /**
     * Test that changes in cache affect index, and vice versa.
     */
    @Test
    public void testIndexState() {
        IgniteCache<KeyClass, ValueClass> cache = cache();

        assertColumnValues(1L, 2L, 3L);

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)"));

        assertColumnValues(1L, 2L, 3L);

        cache.remove(new KeyClass(2));

        assertColumnValues(1L, 3L);

        cache.put(new KeyClass(0), new ValueClass(0L));

        assertColumnValues(0L, 1L, 3L);

        cache.query(new SqlFieldsQuery("DROP INDEX \"" + IDX_NAME_1_ESCAPED + "\""));

        assertColumnValues(0L, 1L, 3L);
    }

    /**
     * Check that values of {@code field1} match what we expect.
     *
     * @param vals Expected values.
     */
    private void assertColumnValues(Long... vals) {
        List<List<?>> expRes = new ArrayList<>(vals.length);

        for (Long v : vals)
            expRes.add(Collections.singletonList(v));

        List<List<?>> all = cache().query(new SqlFieldsQuery("SELECT \"" + FIELD_NAME_1_ESCAPED + "\" FROM \"" +
            TBL_NAME_ESCAPED + "\" ORDER BY \"id\"")).getAll();
        assertEquals(expRes, all);
    }

    /**
     * Do a {@code SELECT COUNT(*)} query to check index state correctness.
     *
     * @param expSize Expected number of items in table.
     */
    private void assertSize(long expSize) {
        assertEquals(expSize, cache().size());

        Object actual = cache().query(new SqlFieldsQuery("SELECT COUNT(*) from \"ValueClass\""))
            .getAll().get(0).get(0);
        assertEquals(expSize, actual);
    }

    /**
     * Get configurations to be used in test.
     *
     * @return Configurations.
     * @throws Exception If failed.
     */
    private List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
            serverConfiguration(0),
            serverConfiguration(1),
            clientConfiguration(2),
            serverConfiguration(3)
        );
    }

    /**
     * @return Client node.
     */
    private Ignite client() {
        return ignite(CLIENT);
    }

    /**
     * @return Cache.
     */
    private IgniteCache<KeyClass, ValueClass> cache() {
        return client().cache(CACHE_NAME);
    }

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return commonConfiguration(idx);
    }

    /**
     * Create client configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration<KeyClass, ValueClass> ccfg = new CacheConfiguration<KeyClass, ValueClass>()
            .setName(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(KeyClass.class.getName());
        entity.setValueType(ValueClass.class.getName());

        entity.addQueryField("id", Long.class.getName(), null);
        entity.addQueryField(FIELD_NAME_1_ESCAPED, Long.class.getName(), null);
        entity.addQueryField(FIELD_NAME_2_ESCAPED, Long.class.getName(), null);

        entity.setKeyFields(Collections.singleton("id"));

        entity.setAliases(Collections.singletonMap(FIELD_NAME_2_ESCAPED, alias(FIELD_NAME_2_ESCAPED)));

        ccfg.setQueryEntities(Collections.singletonList(entity));

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setSqlEscapeAll(true);
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setCacheMode(cacheMode());

        if (nearCache())
            ccfg.setNearConfiguration(new NearCacheConfiguration<KeyClass, ValueClass>());

        return ccfg;
    }

    /**
     * @return Cache mode to use.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Cache atomicity mode to use.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Whether to use near cache.
     */
    protected abstract boolean nearCache();
}
