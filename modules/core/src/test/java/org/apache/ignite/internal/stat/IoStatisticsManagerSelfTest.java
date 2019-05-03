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
import org.junit.Test;

import static org.apache.ignite.internal.stat.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;

/**
 * Tests for IO statistic manager.
 */
public class IoStatisticsManagerSelfTest extends GridCommonAbstractTest {

    /** */
    private static final int RECORD_COUNT = 5000;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test existing zero statistics for not touched caches.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testEmptyIOStat() throws Exception {
        IgniteEx ign = prepareIgnite(true);

        IoStatisticsManager ioStatMgr = ign.context().ioStats();

        Map<IoStatisticsHolderKey, IoStatisticsHolder> stat = ioStatMgr.statistics(IoStatisticsType.CACHE_GROUP);

        checkEmptyStat(stat, DEFAULT_CACHE_NAME, null);

        stat = ioStatMgr.statistics(IoStatisticsType.HASH_INDEX);

        checkEmptyStat(stat, DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME);
    }

    /**
     * @param stat Statistics map.
     * @param name Name of statistics.
     * @param subName Subname of statistics.
     */
    private void checkEmptyStat(Map<IoStatisticsHolderKey, IoStatisticsHolder> stat, String name, String subName) {
        assertEquals(1, stat.size());

        IoStatisticsHolder cacheIoStatHolder = stat.get(new IoStatisticsHolderKey(name, subName));

        assertNotNull(cacheIoStatHolder);

        assertEquals(0, cacheIoStatHolder.logicalReads());

        assertEquals(0, cacheIoStatHolder.physicalReads());
    }

    /**
     * Test LOCAL_NODE statistics tracking for persistent cache.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testNotPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(false);
    }

    /**
     * Test LOCAL_NODE statistics tracking for not persistent cache.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(true);
    }

    /**
     * Check LOCAL_NODE statistics tracking.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @throws Exception In case of failure.
     */
    private void ioStatGlobalPageTrackTest(boolean isPersistent) throws Exception {
        IoStatisticsManager ioStatMgr = prepareData(isPersistent);

        long physicalReadsCnt = ioStatMgr.physicalReads(IoStatisticsType.CACHE_GROUP, DEFAULT_CACHE_NAME, null);

        if (isPersistent)
            Assert.assertTrue(physicalReadsCnt>0);
        else
            Assert.assertEquals(0, physicalReadsCnt);

        Long logicalReads = ioStatMgr.logicalReads(IoStatisticsType.HASH_INDEX, DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME);

        Assert.assertNotNull(logicalReads);

        Assert.assertEquals(RECORD_COUNT, logicalReads.longValue());
    }

    /**
     * Prepare Ignite instance and fill cache.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @return IO statistic manager.
     * @throws Exception In case of failure.
     */
    @NotNull private IoStatisticsManager prepareData(boolean isPersistent) throws Exception {
        IgniteEx ign = prepareIgnite(isPersistent);

        IoStatisticsManager ioStatMgr = ign.context().ioStats();

        IgniteCache cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        ioStatMgr.reset();

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
                        .setMaxSize(30L * 1024 * 1024)
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
