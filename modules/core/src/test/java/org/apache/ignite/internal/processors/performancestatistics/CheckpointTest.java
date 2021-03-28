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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Tests checkpoint performance statistics.
 */
public class CheckpointTest extends AbstractPerformanceStatisticsTest {
    /** Cache entry count. */
    private static final int ENTRY_COUNT = 100;

    /** Ignite. */
    private static IgniteEx srv;

    /** Test cache. */
    private static IgniteCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        cache = srv.cache(DEFAULT_CACHE_NAME);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCheckpoint() throws Exception {
        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        for (int i = 0; i < ENTRY_COUNT; i++)
            cache.put(i, i);

        srv.cluster().state(ClusterState.INACTIVE);

        AtomicBoolean checked = new AtomicBoolean(false);

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void checkpoint(long startTime, long totalDuration, long beforeLockDuration, long duration,
                long execDuration, long holdDuration, long fsyncDuration, long entryDuration, long pagesDuration,
                long pagesSize) {
                checked.set(true);
            }
        });

        assertTrue(checked.get());
    }
}
