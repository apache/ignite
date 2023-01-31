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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.management.DynamicMBean;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.junit.Assert.assertNotEquals;

/**
 * The test shows that the WalWritingRate metric is not calculated when walMode is in (LOG_ONLY, BACKGROUND).
 */
@RunWith(Parameterized.class)
public class IoDatastorageMetricsTest extends GridCommonAbstractTest {

    /**
     * WALMode.
     */
    @Parameterized.Parameter
    public WALMode walMode;

    /**
     * WALMode values.
     */
    @Parameterized.Parameters(name = "walMode={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {WALMode.FSYNC},
            new Object[] {WALMode.BACKGROUND},
            new Object[] {WALMode.LOG_ONLY}
        );
    }

    /**
     * Тumber of segments.
     */
    private static final int MAX_SEGMENTS = 20;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setMetricsEnabled(true)
                    .setWalArchivePath(DFLT_WAL_PATH)
                    .setWalSegments(10)
                    .setWalSegmentSize(1 * 1024 * 1024)
                    .setMaxWalArchiveSize(20 * 1024 * 1024)
                    .setWalMode(walMode)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(20 * 1024 * 1024)
                            .setCheckpointPageBufferSize(2 * 1024 * 1024)
                    )
            );
    }

    /**
     * The test shows that the WalWritingRate metric is not calculated when walMode is in (LOG_ONLY, BACKGROUND).
     * After the tests are completed, you can see the report with grep by ">>> REPORT"
     */
    @Test
    public void WalWritingRate() throws Exception {
        IgniteEx ignite = startGrid(0);
        ignite.cluster().state(ClusterState.ACTIVE);

        AtomicLong key = new AtomicLong();
        List<Long> walWritingRates = new ArrayList<>();

        long lastSegment = walMgr(ignite).currentSegment() + MAX_SEGMENTS;
        long prewSegment = walMgr(ignite).currentSegment();

        while (walMgr(ignite).currentSegment() < lastSegment) {
            long k = key.getAndIncrement();
            long[] arr = new long[64];

            Arrays.fill(arr, k);

            ignite.cache(DEFAULT_CACHE_NAME).put(key, arr);

            if (prewSegment != walMgr(ignite).currentSegment()) {
                prewSegment = walMgr(ignite).currentSegment();
                DynamicMBean dataRegionMBean = metricRegistry(ignite.name(), "io", "datastorage");
                walWritingRates.add((Long)dataRegionMBean.getAttribute("WalWritingRate"));
            }
        }
        long summ = walWritingRates.stream().collect(Collectors.summingLong(Long::longValue));
        assertNotEquals(0L, summ);
    }
}
