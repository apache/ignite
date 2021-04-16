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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.QueryParserMetricsHolder.QUERY_PARSER_METRIC_GROUP_NAME;

/**
 * Test to check {@link QueryParserMetricsHolder}
 */
public class QueryParserMetricsHolderSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Ignite. */
    private static IgniteEx ignite;

    /** Cache. */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid();
        cache = ignite.getOrCreateCache(new CacheConfiguration<>(CACHE_NAME));
    }

    /**
     * Ensure that query cache hits statistic is properly collected
     */
    @Test
    public void testParserCacheHits() {
        LongMetric hits = (LongMetric)ignite.context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME).findMetric("hits");

        Assert.assertNotNull("Unable to find metric with name " + QUERY_PARSER_METRIC_GROUP_NAME + ".hits", hits);

        hits.reset();

        cache.query(new SqlFieldsQuery("CREATE TABLE tbl_hits (id LONG PRIMARY KEY, val LONG)"));

        Assert.assertEquals(0, hits.value());

        for (int i = 0; i < 10; i++)
            cache.query(new SqlFieldsQuery("INSERT INTO tbl_hits (id, val) values (?, ?)").setArgs(i, i));

        Assert.assertEquals(9, hits.value());

        cache.query(new SqlFieldsQuery("SELECT * FROM tbl_hits"));

        Assert.assertEquals(9, hits.value());

        cache.query(new SqlFieldsQuery("SELECT * FROM tbl_hits"));

        Assert.assertEquals(10, hits.value());
    }

    /**
     * Ensure that query cache misses statistic is properly collected
     */
    @Test
    public void testParserCacheMisses() {
        LongMetric misses = ignite.context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME).findMetric("misses");

        Assert.assertNotNull("Unable to find metric with name " + QUERY_PARSER_METRIC_GROUP_NAME + ".misses", misses);

        misses.reset();

        cache.query(new SqlFieldsQuery("CREATE TABLE tbl_misses (id LONG PRIMARY KEY, val LONG)"));

        Assert.assertEquals(1, misses.value());

        for (int i = 0; i < 10; i++)
            cache.query(new SqlFieldsQuery("INSERT INTO tbl_misses (id, val) values (?, ?)").setArgs(i, i));

        Assert.assertEquals(2, misses.value());

        cache.query(new SqlFieldsQuery("SELECT * FROM tbl_misses"));

        Assert.assertEquals(3, misses.value());

        cache.query(new SqlFieldsQuery("SELECT * FROM tbl_misses"));

        Assert.assertEquals(3, misses.value());
    }
}
