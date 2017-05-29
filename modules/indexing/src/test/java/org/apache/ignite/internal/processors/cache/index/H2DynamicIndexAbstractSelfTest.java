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
import javax.cache.CacheException;
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
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Test that checks indexes handling on H2 side.
 */
public abstract class H2DynamicIndexAbstractSelfTest extends AbstractSchemaSelfTest {
    /** Client node index. */
    private final static int CLIENT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        client().getOrCreateCache(cacheConfiguration());

        assertNoIndex(CACHE_NAME, TBL_NAME_ESCAPED, IDX_NAME_1_ESCAPED);

        IgniteCache<KeyClass, ValueClass> cache = client().cache(CACHE_NAME);

        cache.put(new KeyClass(1), new ValueClass("val1"));
        cache.put(new KeyClass(2), new ValueClass("val2"));
        cache.put(new KeyClass(3), new ValueClass("val3"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        client().destroyCache(CACHE_NAME);

        super.afterTest();
    }

    /**
     * Test that after index creation index is used by queries.
     */
    public void testCreateIndex() throws Exception {
        IgniteCache<KeyClass, ValueClass> cache = cache();

        assertSize(3);

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)")).getAll();

        // Test that local queries on all nodes use new index.
        for (int i = 0 ; i < 4; i++) {
            List<List<?>> locRes = ignite(i).cache("cache").query(new SqlFieldsQuery("explain select \"id\" from " +
                "\"cache\".\"ValueClass\" where \"field1\" = 'A'").setLocal(true)).getAll();

            assertEquals(F.asList(
                Collections.singletonList("SELECT\n" +
                    "    \"id\"\n" +
                    "FROM \"cache\".\"ValueClass\"\n" +
                    "    /* \"cache\".\"idx_1\": \"field1\" = 'A' */\n" +
                    "WHERE \"field1\" = 'A'")
            ), locRes);
        }

        assertSize(3);

        cache.remove(new KeyClass(2));

        assertSize(2);

        cache.put(new KeyClass(4), new ValueClass("someVal"));

        assertSize(3);
    }

    /**
     * Test that creating an index with duplicate name yields an error.
     */
    public void testCreateIndexWithDuplicateName() {
        final IgniteCache<KeyClass, ValueClass> cache = cache();

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)"));

        assertSqlException(new RunnableX() {
            @Override public void run() throws Exception {
                cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" +
                    TBL_NAME_ESCAPED + "\"(\"id\" ASC)"));
            }
        }, IgniteQueryErrorCode.INDEX_ALREADY_EXISTS);
    }

    /**
     * Test that creating an index with duplicate name does not yield an error with {@code IF NOT EXISTS}.
     */
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
    public void testDropIndex() {
        IgniteCache<KeyClass, ValueClass> cache = cache();

        assertSize(3);

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)"));

        assertSize(3);

        cache.query(new SqlFieldsQuery("DROP INDEX \"" + IDX_NAME_1_ESCAPED + "\""));

        // Test that no local queries on all nodes use new index.
        for (int i = 0 ; i < 4; i++) {
            List<List<?>> locRes = ignite(i).cache("cache").query(new SqlFieldsQuery("explain select \"id\" from " +
                "\"cache\".\"ValueClass\" where \"field1\" = 'A'").setLocal(true)).getAll();

            assertEquals(F.asList(
                Collections.singletonList("SELECT\n" +
                    "    \"id\"\n" +
                    "FROM \"cache\".\"ValueClass\"\n" +
                    "    /* \"cache\".\"ValueClass\".__SCAN_ */\n" +
                    "WHERE \"field1\" = 'A'")
            ), locRes);
        }

        assertSize(3);
    }

    /**
     * Test that dropping a non-existent index yields an error.
     */
    public void testDropMissingIndex() {
        final IgniteCache<KeyClass, ValueClass> cache = cache();

        assertSqlException(new RunnableX() {
            @Override public void run() throws Exception {
                cache.query(new SqlFieldsQuery("DROP INDEX \"" + IDX_NAME_1_ESCAPED + "\""));
            }
        }, IgniteQueryErrorCode.INDEX_NOT_FOUND);
    }

    /**
     * Test that dropping a non-existent index does not yield an error with {@code IF EXISTS}.
     */
    public void testDropMissingIndexIfExists() {
        final IgniteCache<KeyClass, ValueClass> cache = cache();

        cache.query(new SqlFieldsQuery("DROP INDEX IF EXISTS \"" + IDX_NAME_1_ESCAPED + "\""));
    }

    /**
     * Test that changes in cache affect index, and vice versa.
     */
    public void testIndexState() {
        IgniteCache<KeyClass, ValueClass> cache = cache();

        assertColumnValues("val1", "val2", "val3");

        cache.query(new SqlFieldsQuery("CREATE INDEX \"" + IDX_NAME_1_ESCAPED + "\" ON \"" + TBL_NAME_ESCAPED + "\"(\""
            + FIELD_NAME_1_ESCAPED + "\" ASC)"));

        assertColumnValues("val1", "val2", "val3");

        cache.remove(new KeyClass(2));

        assertColumnValues("val1", "val3");

        cache.put(new KeyClass(0), new ValueClass("someVal"));

        assertColumnValues("someVal", "val1", "val3");

        cache.query(new SqlFieldsQuery("DROP INDEX \"" + IDX_NAME_1_ESCAPED + "\""));

        assertColumnValues("someVal", "val1", "val3");
    }

    /**
     * Check that values of {@code field1} match what we expect.
     * @param vals Expected values.
     */
    private void assertColumnValues(String... vals) {
        List<List<?>> expRes = new ArrayList<>(vals.length);

        for (String v : vals)
            expRes.add(Collections.singletonList(v));

        assertEquals(expRes, cache().query(new SqlFieldsQuery("SELECT \"" + FIELD_NAME_1_ESCAPED + "\" FROM \"" +
            TBL_NAME_ESCAPED + "\" ORDER BY \"id\"")).getAll());
    }

    /**
     * Do a {@code SELECT COUNT(*)} query to check index state correctness.
     * @param expSize Expected number of items in table.
     */
    private void assertSize(long expSize) {
        assertEquals(expSize, cache().size());

        assertEquals(expSize, cache().query(new SqlFieldsQuery("SELECT COUNT(*) from \"ValueClass\""))
            .getAll().get(0).get(0));
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
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setMarshaller(new BinaryMarshaller());

        return optimize(cfg);
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
        entity.addQueryField(FIELD_NAME_1_ESCAPED, String.class.getName(), null);
        entity.addQueryField(FIELD_NAME_2_ESCAPED, String.class.getName(), null);

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

    /**
     * Ensure that SQL exception is thrown.
     *
     * @param r Runnable.
     * @param expCode Error code.
     */
    private static void assertSqlException(DynamicIndexAbstractBasicSelfTest.RunnableX r, int expCode) {
        try {
            try {
                r.run();
            }
            catch (CacheException e) {
                if (e.getCause() != null)
                    throw (Exception)e.getCause();
                else
                    throw e;
            }
        }
        catch (IgniteSQLException e) {
            assertEquals("Unexpected error code [expected=" + expCode + ", actual=" + e.statusCode() + ']',
                expCode, e.statusCode());

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(IgniteSQLException.class.getSimpleName() +  " is not thrown.");
    }
}
