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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils.SystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

/**
 * Tests for IO statistic manager.
 */
public class GridIoStatManagerTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test statistics for persistent cache with enabled statistics tracking.
     *
     * @throws Exception In case of failure.
     */
    public void testNotPersistentIOStatEnabled() throws Exception {
        ioStatPageTrackEnabledTest(false);
    }

    /**
     * Test statistics for not persistent cache with enabled statistics tracking.
     *
     * @throws Exception In case of failure.
     */
    public void testPersistentIOStatEnabled() throws Exception {
        ioStatPageTrackEnabledTest(true);
    }

    /**
     * Test statistics for persistent cache with disabled statistics tracking.
     *
     * @throws Exception In case of failure.
     */
    public void testPersistentIOStatDisabled() throws Exception {
        ioStatPageTrackDisabledTest(true);
    }

    /**
     * Test statistics for not persistent cache with disabled statistics tracking.
     *
     * @throws Exception In case of failure.
     */
    public void testNotPersistentIOStatDisabled() throws Exception {
        ioStatPageTrackDisabledTest(false);
    }

    /**
     * Check statistics for enabled statistics tracking.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @throws Exception In case of failure.
     */
    public void ioStatPageTrackEnabledTest(boolean isPersistent) throws Exception {
        try (SystemProperty ignored = new SystemProperty(IgniteSystemProperties.IGNITE_PAGE_TRACK_LOG_ENABLE, "true")) {
            GridIoStatManager ioStatMgr = prepareData(isPersistent);

            if (isPersistent) {
                Assert.assertFalse(ioStatMgr.physicalReads().isEmpty());

                Assert.assertFalse(ioStatMgr.physicalWrites().isEmpty());
            }
            else {
                Assert.assertTrue(ioStatMgr.physicalReads().isEmpty());

                Assert.assertTrue(ioStatMgr.physicalWrites().isEmpty());

            }

            Assert.assertTrue(ioStatMgr.logicalReads().containsKey(GridIoStatManager.PageType.DATA));
            Assert.assertTrue(ioStatMgr.logicalReads().get(GridIoStatManager.PageType.DATA) > 0);
        }
    }

    /**
     * Check statistics for disabled statistics tracking.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @throws Exception In case of failure.
     */
    public void ioStatPageTrackDisabledTest(boolean isPersistent) throws Exception {
        try (SystemProperty ignored = new SystemProperty(IgniteSystemProperties.IGNITE_PAGE_TRACK_LOG_ENABLE, "false")) {
            GridIoStatManager ioStatMgr = prepareData(isPersistent);

            Assert.assertTrue(ioStatMgr.physicalReads().isEmpty());

            Assert.assertTrue(ioStatMgr.physicalWrites().isEmpty());

            Assert.assertTrue(ioStatMgr.logicalReads().isEmpty());
        }
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

        for (int i = 0; i < 1000; i++)
            cache.put("KEY-" + i, "VAL-" + i);

        ioStatMgr.logStats();

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
