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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that WAL compaction works correctly in no-archiver mode.
 */
public class WalCompactionNoArchiverTest extends GridCommonAbstractTest {
    /** Wal segment size. */
    private static final int WAL_SEGMENT_SIZE = 4 * 1024 * 1024;

    /** Cache name. */
    public static final String CACHE_NAME = "cache";

    /** Entries count. */
    public static final int ENTRIES = 1000;

    /** WAL path. */
    public static final String WAL_PATH = "no-arch";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(200L * 1024 * 1024))
            .setWalSegmentSize(WAL_SEGMENT_SIZE)
            .setWalCompactionEnabled(true)
            .setWalArchivePath(WAL_PATH)
            .setWalPath(WAL_PATH)
            .setCheckpointFrequency(1000));

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), WAL_PATH, true));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), WAL_PATH, true));
    }

    /**
     * Tests that attempts to compress WAL segment don't result with error.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_WAL_COMPRESSOR_WORKER_THREAD_CNT", value = "1")
    public void testNoCompressionErrors() throws Exception {
        IgniteEx ig = startGrid(0);
        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache(CACHE_NAME);

        for (int i = 0; i < ENTRIES; i++) { // At least 20MB of raw data in total.
            final byte[] val = new byte[40000];

            val[i] = 1;

            cache.put(i, val);
        }

        stopAllGrids();

        ig = startGrid(0);

        cache = ig.cache(CACHE_NAME);

        for (int i = 0; i < ENTRIES; i++) { // At least 20MB of raw data in total.
            final byte[] val = new byte[40000];

            val[i] = 1;

            cache.put(i, val);
        }

        IgniteWriteAheadLogManager wal = ig.context().cache().context().wal();

        Object compressor = U.field(wal, "compressor");

        assertNotNull(compressor);

        Object error = U.field(compressor, "lastCompressionError");

        if (error != null)
            fail("Unexpected error in FileCompressor: " + error);
    }
}
