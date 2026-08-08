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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_TEMP_FILE_COMPACTED_FILTER;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Load without compaction -> Stop -> Enable WAL Compaction -> Start.
 */
public class WalCompactionSwitchOnTest extends GridCommonAbstractTest {
    /** Compaction enabled. */
    private boolean compactionEnabled;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(256 * 1024 * 1024)
            )
            .setWalSegmentSize(512 * 1024)
            .setWalSegments(100)
            .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE)
            .setWalCompactionEnabled(compactionEnabled);

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
    }

    /**
     * Load without compaction -> Stop -> Enable WAL Compaction -> Start.
     *
     * @throws Exception On exception.
     */
    @Test
    public void testWalCompactionSwitch() throws Exception {
        IgniteEx n = startGrid(0);
        n.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = n.cache(DEFAULT_CACHE_NAME);
        for (int i = 0; i < 10_000; i++)
            cache.put(i, i); // Streamer is intentionally not used to ensure there are more WalRecords.

        File walArchiveDir = fileTree(n).walArchive();
        assertNotNull(walArchiveDir);

        forceCheckpoint();
        assertTrue(waitForCondition(() -> collectAndSortWalSegments(walArchiveDir).length >= 10, 5_000, 32));

        stopGrid(0);

        assertTrue(collectAndSortWalSegments(walArchiveDir).length >= 10);
        assertTrue(collectAndSortCompactedWalSegments(walArchiveDir).length == 0);

        compactionEnabled = true;
        startGrid(0);

        assertTrue(waitForCondition(() -> collectAndSortCompactedWalSegments(walArchiveDir).length >= 5, 5_000, 32));

        File minTmpFile = minTmpWalSegemnt(walArchiveDir);
        // Checks that tmp files of compacted WAL segments are missing or have been deleted for compacted ones.
        assertTrue(Objects.toString(minTmpFile), minTmpFile == null || walSegmentIdx(minTmpFile) >= 5);
    }

    /** */
    private static NodeFileTree fileTree(IgniteEx n) {
        return n.context().pdsFolderResolver().fileTree();
    }

    /** */
    private static @Nullable File minTmpWalSegemnt(File dir) {
        return Stream.of(dir.listFiles(WAL_SEGMENT_TEMP_FILE_COMPACTED_FILTER))
            .min(Comparator.comparingLong(WalCompactionSwitchOnTest::walSegmentIdx))
            .orElse(null);
    }

    /** */
    private static File[] collectAndSortWalSegments(File dir) {
        return sortWalSegments(dir.listFiles(f -> NodeFileTree.walSegment(f)));
    }

    /** */
    private static File[] collectAndSortCompactedWalSegments(File dir) {
        return sortWalSegments(dir.listFiles(NodeFileTree::walCompactedSegment));
    }

    /** */
    private static File[] sortWalSegments(File... walSegments) {
        return Stream.of(walSegments)
            .sorted(Comparator.comparingLong(f -> U.fixedLengthFileNumber(f.getName())))
            .toArray(File[]::new);
    }

    /** */
    private static long walSegmentIdx(File f) {
        return U.fixedLengthFileNumber(f.getName());
    }
}
