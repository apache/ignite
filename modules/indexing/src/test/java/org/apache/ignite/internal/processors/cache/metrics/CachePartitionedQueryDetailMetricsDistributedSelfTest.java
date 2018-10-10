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

package org.apache.ignite.internal.processors.cache.metrics;

import org.apache.ignite.configuration.CacheConfiguration;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for partitioned distributed cache query metrics.
 */
public class CachePartitionedQueryDetailMetricsDistributedSelfTest extends CacheAbstractQueryDetailMetricsSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        gridCnt = 2;
        cacheMode = PARTITIONED;

        super.beforeTest();
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, String> configureCache2(String cacheName) {
        CacheConfiguration<Integer, String> ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setIndexedTypes(Integer.class, Long.class);
        ccfg.setStatisticsEnabled(true);
        ccfg.setQueryDetailMetricsSize(QRY_DETAIL_METRICS_SIZE);

        return ccfg;
    }
}
