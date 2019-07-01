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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.metric.MetricGroup;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** */
@RunWith(Parameterized.class)
public class CacheMetricsAddRemoveTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_GETS = "CacheGets";

    /** */
    public static final String CACHE_PUTS = "CachePuts";

    /** Cache modes. */
    @Parameterized.Parameters(name = "cacheMode={0},nearEnabled={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
            new Object[] {CacheMode.PARTITIONED, false},
            new Object[] {CacheMode.PARTITIONED, true},
            new Object[] {CacheMode.REPLICATED, false},
            new Object[] {CacheMode.REPLICATED, true}
        );
    }

    /** . */
    @Parameterized.Parameter(0)
    public CacheMode mode;

    /** Use index. */
    @Parameterized.Parameter(1)
    public boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);

        IgniteConfiguration clientCfg = getConfiguration("client")
            .setClientMode(true);

        startGrid(clientCfg);
    }

    /** */
    @Test
    public void testCacheMetricsAddRemove() throws Exception {
        String cachePrefix = metricName("cache", DEFAULT_CACHE_NAME);

        checkMetricsEmpty(cachePrefix);

        createCache();

        checkMetricsNotEmpty(cachePrefix);

        destroyCache();

        checkMetricsEmpty(cachePrefix);
    }

    /** */
    private void destroyCache() throws InterruptedException {
        grid("client").destroyCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();
    }

    /** */
    private void createCache() throws InterruptedException {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        if (nearEnabled)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        grid("client").createCache(ccfg);

        awaitPartitionMapExchange();
    }

    /** */
    private void checkMetricsNotEmpty(String cachePrefix) {
        for (int i=0; i<2; i++) {
            MetricRegistry mreg = metricRegistry(i);

            MetricGroup mgrp = mreg.group(cachePrefix);

            assertNotNull(mgrp.findMetric(CACHE_GETS));
            assertNotNull(mgrp.findMetric(CACHE_PUTS));

            if (nearEnabled) {
                mgrp = mreg.group(metricName("cache", DEFAULT_CACHE_NAME, "near"));

                assertNotNull(mgrp.findMetric(CACHE_GETS));
                assertNotNull(mgrp.findMetric(CACHE_PUTS));
            }
        }
    }

    /** */
    private void checkMetricsEmpty(String cachePrefix) {
        for (int i=0; i<3; i++) {
            MetricRegistry mreg = metricRegistry(i);

            MetricGroup mgrp = mreg.group(cachePrefix);

            assertNull(mgrp.findMetric(metricName(cachePrefix, CACHE_GETS)));
            assertNull(mgrp.findMetric(metricName(cachePrefix, CACHE_PUTS)));

            if (nearEnabled) {
                mgrp = mreg.group(metricName("cache", DEFAULT_CACHE_NAME, "near"));

                assertNull(mgrp.findMetric(CACHE_GETS));
                assertNull(mgrp.findMetric(CACHE_PUTS));
            }
        }
    }

    /** */
    private MetricRegistry metricRegistry(int gridIdx) {
        MetricRegistry mreg;
        if (gridIdx < 2)
            mreg = grid(0).context().metric().registry();
        else
            mreg = grid("client").context().metric().registry();
        return mreg;
    }
}
