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
 * Tests BPlusTree metrics.
 */
public class BPlusTreeMetricsTest extends AbstractIndexingCommonTest {
    /** */
    private static final String INSERT_CNT = "InsertCount";

    /** */
    private static final String REMOVE_CNT = "RemoveFromLeafCount";

    /** */
    private static final String SEARCH_CNT = "SearchCount";

    /** */
    private static final String INSERT_TIME = "InsertTime";

    /** */
    private static final String REMOVE_TIME = "RemoveFromLeafTime";

    /** */
    private static final String SEARCH_TIME = "SearchTime";

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration<Integer, TestClass> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, TestClass> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setIndexedTypes(Integer.class, TestClass.class);
        ccfg.setStatisticsEnabled(true);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test metrics exposed by index BPlusTree.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValues() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, TestClass> cache = ignite.getOrCreateCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        ReadOnlyMetricRegistry intFieldReg = findRegistry(ignite, "intField");
        ReadOnlyMetricRegistry strFieldReg = findRegistry(ignite, "strField");

        assertEquals(0, metric(intFieldReg, INSERT_CNT));
        assertEquals(0, metric(strFieldReg, INSERT_CNT));
        assertEquals(0, metric(intFieldReg, INSERT_TIME));
        assertEquals(0, metric(strFieldReg, INSERT_TIME));

        for (int i = 0; i < 5000; i++)
            cache.put(i, new TestClass(i));

        // This metric includes insert to inner nodes, so value can exceed count of inserted keys.
        assertTrue(metric(intFieldReg, INSERT_CNT) > 5000);
        assertTrue(metric(strFieldReg, INSERT_CNT) > 5000);
        assertTrue(metric(intFieldReg, INSERT_TIME) > 0);
        assertTrue(metric(strFieldReg, INSERT_TIME) > 0);

        assertEquals(0, metric(intFieldReg, REMOVE_CNT));
        assertEquals(0, metric(strFieldReg, REMOVE_CNT));
        assertEquals(0, metric(intFieldReg, REMOVE_TIME));
        assertEquals(0, metric(strFieldReg, REMOVE_TIME));

        for (int i = 0; i < 1000; i++)
            cache.remove(i);

        assertEquals(1000, metric(intFieldReg, REMOVE_CNT));
        assertEquals(1000, metric(strFieldReg, REMOVE_CNT));
        assertTrue(metric(intFieldReg, REMOVE_TIME) > 0);
        assertTrue(metric(strFieldReg, REMOVE_TIME) > 0);

        long searchOnIntCnt = metric(intFieldReg, SEARCH_CNT);
        long searchOnStrCnt = metric(strFieldReg, SEARCH_CNT);
        long searchOnIntTime = metric(intFieldReg, SEARCH_TIME);
        long searchOnStrTime = metric(strFieldReg, SEARCH_TIME);

        cache.query(new SqlFieldsQuery("SELECT * FROM TESTCLASS WHERE INTFIELD = ?").setArgs(3000)).getAll();

        assertTrue(metric(intFieldReg, SEARCH_CNT) > searchOnIntCnt);
        assertEquals(searchOnStrCnt, metric(strFieldReg, SEARCH_CNT));
        assertTrue(metric(intFieldReg, SEARCH_TIME) > searchOnIntTime);
        assertEquals(searchOnStrTime, metric(strFieldReg, SEARCH_TIME));
    }

    /**
     * Test metrics when cache statistics is disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDisableStatistics() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, TestClass> cache = ignite.getOrCreateCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        ReadOnlyMetricRegistry intFieldReg = findRegistry(ignite, "intField");

        cache.enableStatistics(false);

        cache.put(0, new TestClass(0));

        assertEquals(0, metric(intFieldReg, INSERT_CNT));
        assertEquals(0, metric(intFieldReg, INSERT_TIME));

        cache.query(new SqlFieldsQuery("SELECT * FROM TESTCLASS WHERE INTFIELD = ?").setArgs(0)).getAll();

        assertEquals(0, metric(intFieldReg, SEARCH_CNT));
        assertEquals(0, metric(intFieldReg, SEARCH_TIME));

        cache.remove(0);

        assertEquals(0, metric(intFieldReg, REMOVE_CNT));
        assertEquals(0, metric(intFieldReg, REMOVE_TIME));

        cache.enableStatistics(true);

        cache.put(0, new TestClass(0));

        assertTrue(metric(intFieldReg, INSERT_CNT) > 0);
        assertTrue(metric(intFieldReg, INSERT_TIME) > 0);

        cache.query(new SqlFieldsQuery("SELECT * FROM TESTCLASS WHERE INTFIELD = ?").setArgs(0)).getAll();

        assertTrue(metric(intFieldReg, SEARCH_CNT) > 0);
        assertTrue(metric(intFieldReg, SEARCH_TIME) > 0);

        cache.remove(0);

        assertTrue(metric(intFieldReg, REMOVE_CNT) > 0);
        assertTrue(metric(intFieldReg, REMOVE_TIME) > 0);
    }

    /**
     * Test disable metrics by system property.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_BPLUS_TREE_DISABLE_METRICS, value = "true")
    public void testDisableMetrics() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.getOrCreateCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        for (ReadOnlyMetricRegistry reg : ignite.context().metric())
            assertFalse(reg.name().startsWith(InlineIndexImpl.INDEX_METRIC_PREFIX));
    }

    /** */
    private ReadOnlyMetricRegistry findRegistry(IgniteEx ignite, String fieldName) {
        for (ReadOnlyMetricRegistry reg : ignite.context().metric()) {
            if (reg.name().startsWith(InlineIndexImpl.INDEX_METRIC_PREFIX)
                && reg.name().toUpperCase().contains(fieldName.toUpperCase()))
                return reg;
        }

        throw new AssertionError("Not found metric registry for index on field " + fieldName);
    }

    /** */
    private long metric(ReadOnlyMetricRegistry reg, String metric) {
        LongMetric m = reg.findMetric(metric);

        if (m == null)
            throw new AssertionError("Not found metric " + metric + " in registry " + reg.name());

        return m.value();
    }

    /** */
    private static class TestClass {
        /** */
        @QuerySqlField(index = true)
        private final int intField;

        /** */
        @QuerySqlField(index = true)
        private final String strField;

        /** */
        public TestClass(int val) {
            intField = val;
            strField = "str" + val;
        }
    }
}
