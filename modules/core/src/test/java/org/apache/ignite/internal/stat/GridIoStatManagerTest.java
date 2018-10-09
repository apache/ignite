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
 *
 */

package org.apache.ignite.internal.stat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

/**
 * Tests for IO statistic manager.
 */
public class GridIoStatManagerTest extends GridCommonAbstractTest {

    /** */
    private static final int RECORD_COUNT = 1000;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test GLOBAL statistics tracking for persistent cache.
     *
     * @throws Exception In case of failure.
     */
    public void testNotPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(false);
    }

    /**
     * Test GLOBAL statistics tracking for not persistent cache.
     *
     * @throws Exception In case of failure.
     */
    public void testPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(true);
    }

    /**
     * Check GLOBAL statistics tracking.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @throws Exception In case of failure.
     */
    private void ioStatGlobalPageTrackTest(boolean isPersistent) throws Exception {
        GridIoStatManager ioStatMgr = prepareData(isPersistent);

        long physicalReadsCnt = ioStatMgr.physicalReadsGlobal().values().stream().reduce(Long::sum).get();

        long physicalWritesCnt = ioStatMgr.physicalWritesGlobal().values().stream().reduce(Long::sum).get();

        if (isPersistent) {
            Assert.assertTrue(physicalReadsCnt>0);

            Assert.assertTrue(physicalWritesCnt > 0);
        }
        else {
            Assert.assertEquals(0, physicalReadsCnt);

            Assert.assertEquals(0, physicalWritesCnt);
        }

        Map<AggregatePageType, AtomicLong> aggLogReads = ioStatMgr.aggregate(ioStatMgr.logicalReadsGlobal());

        Assert.assertTrue(aggLogReads.containsKey(AggregatePageType.INDEX));

        Assert.assertEquals(RECORD_COUNT, aggLogReads.get(AggregatePageType.INDEX).longValue());
    }

    /**
     * Prepare Ignite instance and fill cache.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @return IO statistic manager.
     * @throws Exception In case of failure.
     */
    @NotNull private GridIoStatManager prepareData(boolean isPersistent) throws Exception {
        IgniteEx ign = prepareIgnite(isPersistent);

        GridIoStatManager ioStatMgr = ign.context().ioStats();

        IgniteCache cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        ioStatMgr.resetStats();

        for (int i = 0; i < RECORD_COUNT; i++)
            cache.put("KEY-" + i, "VAL-" + i);

        return ioStatMgr;
    }

    /**
     * Create Ignite configuration.
     *
     * @param isPersist {@code true} in case persistence should be enable.
     * @return Ignite configuration.
     * @throws Exception In case of failure.
     */
    private IgniteConfiguration getConfiguration(boolean isPersist) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        if (isPersist) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(10L * 1024 * 1024)
                        .setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /**
     * Start Ignite grid and create cache.
     *
     * @param isPersist {@code true} in case Ignate should use persistente storage.
     * @return Started Ignite instance.
     * @throws Exception In case of failure.
     */
    private IgniteEx prepareIgnite(boolean isPersist) throws Exception {
        IgniteEx ignite = startGrid(getConfiguration(isPersist));

        ignite.cluster().active(true);

        ignite.createCache(new CacheConfiguration<String, String>(DEFAULT_CACHE_NAME));

        return ignite;
    }
}
