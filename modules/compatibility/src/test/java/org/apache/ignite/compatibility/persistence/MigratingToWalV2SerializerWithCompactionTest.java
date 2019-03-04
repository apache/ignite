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

package org.apache.ignite.compatibility.persistence;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

/**
 * Saves data using previous version of ignite and then load this data using actual version
 */
public class MigratingToWalV2SerializerWithCompactionTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = MigratingToWalV2SerializerWithCompactionTest.class.getSimpleName();

    /** Entries count. */
    private static final int ENTRIES = 300;

    /** Wal segment size. */
    private static final int WAL_SEGMENT_SIZE = 1024 * 1024;

    /** Entry payload size. */
    private static final int PAYLOAD_SIZE = 20000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
            .setWalSegmentSize(WAL_SEGMENT_SIZE)
            .setWalCompactionEnabled(true)
            .setWalMode(WALMode.LOG_ONLY)
            .setWalHistorySize(200);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCompactingOldWalFiles() throws Exception {
        doTestStartupWithOldVersion("2.3.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param ver 3-digits version of ignite
     * @throws Exception If failed.
     */
    private void doTestStartupWithOldVersion(String ver) throws Exception {
        try {
            startGrid(1, ver, new ConfigurationClosure(), new PostStartupClosure());

            stopAllGrids();

            IgniteEx ignite = startGrid(0);

            ignite.active(true);

            IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(TEST_CACHE_NAME);

            for (int i = ENTRIES; i < ENTRIES * 2; i++) {
                final byte[] val = new byte[PAYLOAD_SIZE];

                ThreadLocalRandom.current().nextBytes(val);

                val[i] = 1;

                cache.put(i, val);
            }

            // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
            ignite.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
            ignite.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

            Thread.sleep(15_000); // Time to compress WAL.

            int expCompressedWalSegments = PAYLOAD_SIZE * ENTRIES * 4 / WAL_SEGMENT_SIZE - 1;

            String nodeFolderName = ignite.context().pdsFolderResolver().resolveFolders().folderName();

            File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
            File walDir = new File(dbDir, "wal");
            File archiveDir = new File(walDir, "archive");
            File nodeArchiveDir = new File(archiveDir, nodeFolderName);

            File[] compressedSegments = nodeArchiveDir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.endsWith(".wal.zip");
                }
            });

            final int actualCompressedWalSegments = compressedSegments == null ? 0 : compressedSegments.length;

            assertTrue("expected=" + expCompressedWalSegments + ", actual=" + actualCompressedWalSegments,
                actualCompressedWalSegments >= expCompressedWalSegments);

            stopAllGrids();

            File nodeLfsDir = new File(dbDir, nodeFolderName);
            File cpMarkersDir = new File(nodeLfsDir, "cp");

            File[] cpMarkers = cpMarkersDir.listFiles();

            assertNotNull(cpMarkers);
            assertTrue(cpMarkers.length > 0);

            File cacheDir = new File(nodeLfsDir, "cache-" + TEST_CACHE_NAME);
            File[] partFiles = cacheDir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.startsWith("part");
                }
            });

            assertNotNull(partFiles);
            assertTrue(partFiles.length > 0);

            // Enforce reading WAL from the very beginning at the next start.
            for (File f : cpMarkers)
                f.delete();

            for (File f : partFiles)
                f.delete();

            ignite = startGrid(0);

            ignite.active(true);

            cache = ignite.cache(TEST_CACHE_NAME);

            boolean fail = false;

            // Check that all data is recovered from compacted WAL.
            for (int i = 0; i < ENTRIES * 2; i++) {
                byte[] arr = cache.get(i);

                if (arr == null) {
                    System.out.println(">>> Missing: " + i);

                    fail = true;
                }
                else if (arr[i] != 1) {
                    System.out.println(">>> Corrupted: " + i);

                    fail = true;
                }
            }

            assertFalse(fail);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(0);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheCfg);

            for (int i = 0; i < ENTRIES; i++) { // At least 20MB of raw data in total.
                final byte[] val = new byte[20000];

                ThreadLocalRandom.current().nextBytes(val);

                val[i] = 1;

                cache.put(i, val);
            }
        }
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true))
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setWalMode(WALMode.LOG_ONLY)
                .setWalHistorySize(100);

            cfg.setDataStorageConfiguration(memCfg);
        }
    }
}
