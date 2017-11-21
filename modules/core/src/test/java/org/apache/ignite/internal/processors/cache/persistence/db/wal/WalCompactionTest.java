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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class WalCompactionTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Wal segment size. */
    private static final int WAL_SEGMENT_SIZE = 4 * 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(200 * 1024 * 1024))
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(WAL_SEGMENT_SIZE)
            .setWalHistorySize(500)
            .setWalCompactionEnabled(true));

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("cache");
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalCompaction() throws Exception {
        IgniteEx ig = (IgniteEx)startGrids(3);
        ig.active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache("cache");

        for (int i = 0; i < 1000; i++) {
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val); // At least 20MB of raw data.
        }

        // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        Thread.sleep(15_000); // Allow compressor to archive WAL segments.

        String nodeFolderName = ig.context().pdsFolderResolver().resolveFolders().folderName();

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);
        File walSegment = new File(nodeArchiveDir, FileWriteAheadLogManager.FileDescriptor.fileName(0) + ".zip");

        assertTrue(walSegment.exists());
        assertTrue(walSegment.length() < WAL_SEGMENT_SIZE / 2); // Should be compressed at least in half.

        stopAllGrids();

        File nodeLfsDir = new File(dbDir, nodeFolderName);
        File cpMarkersDir = new File(nodeLfsDir, "cp");

        File[] cpMarkers = cpMarkersDir.listFiles();

        assertNotNull(cpMarkers);
        assertTrue(cpMarkers.length > 0);

        // Enforce reading WAL from the very beginning at the next start.
        for (File f : cpMarkers)
            f.delete();

        ig = (IgniteEx)startGrids(3);
        ig.active(true);

        cache = ig.cache("cache");

        // Check that all data is recovered from compacted WAL.
        for (int i = 0; i < 1000; i++) {
            byte[] arr = cache.get(i);

            assertNotNull(arr);

            assertEquals(1, arr[i]);
        }
    }
}
