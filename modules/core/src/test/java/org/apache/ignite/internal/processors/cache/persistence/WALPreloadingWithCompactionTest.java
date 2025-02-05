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


package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for checking preloading within context of wall reservation and release.
 */
public class WALPreloadingWithCompactionTest extends GridCommonAbstractTest {
    /** Partitions count. */
    private static final int PARTITIONS_COUNT = 16;

    /** WAL segment size. */
    private static final int WAL_SEGMENT_SIZE = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true))
            .setWalSegmentSize(WAL_SEGMENT_SIZE)
            .setWalMode(WALMode.LOG_ONLY)
            .setWalCompactionEnabled(true);

        storageCfg.setWalCompactionLevel(6);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /**
     *
     * @throws Exception if failed.
     */
    @Test
    public void test() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "1");

        startGridsMultiThreaded(2).cluster().state(ClusterState.ACTIVE);

        ignite(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setAffinity(new RendezvousAffinityFunction(false, PARTITIONS_COUNT)).setBackups(1));

        forceCheckpoint();

        for (int i = 0; i < PARTITIONS_COUNT; i++)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        stopGrid(1);

        for (int i = PARTITIONS_COUNT; i < 2 * PARTITIONS_COUNT; i++)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        startGrid(1);

        awaitPartitionMapExchange(true, true, null);

        for (int i = 2 * PARTITIONS_COUNT; i < 500 * PARTITIONS_COUNT; i++)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint();

        for (int i = 500 * PARTITIONS_COUNT; i < 1000 * PARTITIONS_COUNT; i++)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        checkThatOnlyZipSegmentExists(ignite(0), 0);
        checkThatOnlyZipSegmentExists(ignite(1), 0);
    }

    /**
     * Check that there's only compacted version of given segment.
     * @param ignite Ignite instance.
     * @param segment Segment index.
     * @throws IgniteCheckedException If failed.
     */
    private void checkThatOnlyZipSegmentExists(IgniteEx ignite, int segment) throws IgniteCheckedException {
        NodeFileTree ft = ignite.context().pdsFolderResolver().fileTree();

        File walZipSegment = new File(ft.walArchive(), FileDescriptor.fileName(segment) + ".zip");
        File walRawSegment = new File(ft.walArchive(), FileDescriptor.fileName(segment));

        assertTrue(walZipSegment.exists());
        assertFalse(walRawSegment.exists());
    }
}
