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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

/**
 * Tests for inline index tree behavior.
 */
public class InlineIndexTreeTest extends AbstractIndexingCommonTest {
    /** */
    private static final String FULL_ROW_LOAD_CNT = "FullRowLoadCount";

    /** */
    private static final String LONG_STR_IDX = "longStrField_idx";

    /** */
    private static final int ROWS = 5000;

    /** */
    private static final String LONG_STR_PREFIX = "a".repeat(50);

    /**
     * Checks that full row load metric is incremented when inline size is small.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFullRowLoadsMetric() throws Exception {
        TestContext ctx = prepareCluster(4);

        assertEquals(0, metric(ctx.longStrFieldReg));

        insertRows(ctx.cache);

        long before = metric(ctx.longStrFieldReg);

        queryByLongStrField(ctx.cache, 3000);

        long after = metric(ctx.longStrFieldReg);

        assertTrue(after > before);
    }

    /**
     * Checks that full row load metric is not incremented when inline size is big.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFullRowLoadsMetricBigInlineSize() throws Exception {
        TestContext ctx = prepareCluster(128);

        assertEquals(0, metric(ctx.longStrFieldReg));

        insertRows(ctx.cache);

        long before = metric(ctx.longStrFieldReg);

        queryByLongStrField(ctx.cache, 3000);

        long after = metric(ctx.longStrFieldReg);

        assertEquals(before, after);
    }

    /**
     * Checks that put operations do not increment full row loads metric.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetricOnPut() throws Exception {
        TestContext ctx = prepareCluster(4);

        long before = metric(ctx.longStrFieldReg);

        insertRows(ctx.cache);

        long after = metric(ctx.longStrFieldReg);

        assertEquals(before, after);
    }

    /**
     * Checks that full row loads metric is not incremented when cache statistics is disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFullRowLoadMetricStatisticsDisabled() throws Exception {
        TestContext ctx = prepareCluster(4);

        insertRows(ctx.cache);

        ctx.cache.enableStatistics(false);

        queryByLongStrField(ctx.cache, 3000);

        assertEquals(0, metric(ctx.longStrFieldReg));

        ctx.cache.enableStatistics(true);

        queryByLongStrField(ctx.cache, 3000);

        assertTrue(metric(ctx.longStrFieldReg) > 0);
    }

    /**
     * Checks that inline index tree metrics are not registered when metrics are disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_BPLUS_TREE_DISABLE_METRICS, value = "true")
    public void testMetricsDisabled() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, TestClass> cache = ignite.getOrCreateCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        createLongStrIdx(cache, 4);

        for (ReadOnlyMetricRegistry reg : ignite.context().metric())
            assertFalse(reg.name().startsWith(InlineIndexImpl.INDEX_METRIC_PREFIX));
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, TestClass> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, TestClass> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setIndexedTypes(Integer.class, TestClass.class);
        ccfg.setStatisticsEnabled(true);

        return ccfg;
    }

    /**
     * Starts node, creates cache and creates index for long string field.
     *
     * @param inlineSize Inline size.
     * @return Test context.
     * @throws Exception If failed.
     */
    private TestContext prepareCluster(int inlineSize) throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, TestClass> cache = ignite.getOrCreateCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        createLongStrIdx(cache, inlineSize);

        ReadOnlyMetricRegistry longStrFieldReg = findRegistry(ignite, LONG_STR_IDX);

        return new TestContext(cache, longStrFieldReg);
    }

    /**
     * Creates index for long string field with configured inline size.
     *
     * @param cache Cache.
     * @param inlineSize Inline size.
     */
    private void createLongStrIdx(IgniteCache<Integer, TestClass> cache, int inlineSize) {
        cache.query(new SqlFieldsQuery(
            "CREATE INDEX " + LONG_STR_IDX + " ON TESTCLASS(longStrField) INLINE_SIZE " + inlineSize
        )).getAll();
    }

    /**
     * Inserts test rows.
     *
     * @param cache Cache.
     */
    private void insertRows(IgniteCache<Integer, TestClass> cache) {
        for (int i = 0; i < ROWS; i++)
            cache.put(i, new TestClass(i));
    }

    /**
     * Executes query by long string field.
     *
     * @param cache Cache.
     * @param val Value.
     */
    private void queryByLongStrField(IgniteCache<Integer, TestClass> cache, int val) {
        cache.query(new SqlFieldsQuery("SELECT * FROM TESTCLASS WHERE LONGSTRFIELD = ?")
            .setArgs(longStrField(val))).getAll();
    }

    /**
     * Finds index metric registry.
     *
     * @param ignite Ignite node.
     * @param idxName Index name.
     * @return Metric registry.
     */
    private ReadOnlyMetricRegistry findRegistry(IgniteEx ignite, String idxName) {
        for (ReadOnlyMetricRegistry reg : ignite.context().metric()) {
            if (reg.name().startsWith(InlineIndexImpl.INDEX_METRIC_PREFIX) &&
                reg.name().toUpperCase().contains(idxName.toUpperCase()))
                return reg;
        }

        throw new AssertionError("Not found metric registry for index " + idxName);
    }

    /**
     * Returns full row load metric value.
     *
     * @param reg Metric registry.
     * @return Metric value.
     */
    private long metric(ReadOnlyMetricRegistry reg) {
        LongMetric m = reg.findMetric(FULL_ROW_LOAD_CNT);

        if (m == null)
            throw new AssertionError("Not found metric " + FULL_ROW_LOAD_CNT + " in registry " + reg.name());

        return m.value();
    }

    /**
     * @param val Value.
     * @return Long string value.
     */
    private static String longStrField(int val) {
        return LONG_STR_PREFIX + val;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test context.
     */
    private static class TestContext {
        /** */
        private final IgniteCache<Integer, TestClass> cache;

        /** */
        private final ReadOnlyMetricRegistry longStrFieldReg;

        /**
         * @param cache Cache.
         * @param longStrFieldReg Long string field index metric registry.
         */
        private TestContext(
            IgniteCache<Integer, TestClass> cache,
            ReadOnlyMetricRegistry longStrFieldReg
        ) {
            this.cache = cache;
            this.longStrFieldReg = longStrFieldReg;
        }
    }

    /**
     * Test value class.
     */
    private static class TestClass {
        /** */
        @QuerySqlField(index = true)
        private final int intField;

        /** */
        @QuerySqlField
        private final String longStrField;

        /**
         * @param val Value.
         */
        public TestClass(int val) {
            intField = val;
            longStrField = longStrField(val);
        }
    }
}
