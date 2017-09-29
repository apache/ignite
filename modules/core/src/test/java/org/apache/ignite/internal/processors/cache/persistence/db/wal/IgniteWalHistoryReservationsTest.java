/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgniteWalHistoryReservationsTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        MemoryConfiguration memCfg = new MemoryConfiguration();

        long memSize = 200L * 1024L * 1024L;

        memCfg.setMemoryPolicies(
            new MemoryPolicyConfiguration()
                .setInitialSize(memSize)
                .setMaxSize(memSize)
                .setName("dfltMemPlc")
        );

        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(memCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName("cache1");
        ccfg1.setBackups(1);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg1);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        client = false;

        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReservedOnExchange() throws Exception {
        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        final int entryCnt = 10_000;
        final int initGridCnt = 4;

        final IgniteEx ig0 = (IgniteEx)startGrids(initGridCnt);

        ig0.active(true);

        long start = U.currentTimeMillis();

        log.warning("Start loading");

        try (IgniteDataStreamer<Object, Object> st = ig0.dataStreamer("cache1")){
            for (int k = 0; k < entryCnt; k++){
                st.addData(k, k);

                printProgress(k);
            }
        }

        log.warning("Finish loading time:" + (U.currentTimeMillis() - start));

        forceCheckpoint();

        start = U.currentTimeMillis();

        log.warning("Start loading");

        try (IgniteDataStreamer<Object, Object> st = ig0.dataStreamer("cache1")) {
            st.allowOverwrite(true);

            for (int k = 0; k < entryCnt; k++) {
                st.addData(k, k * 2);

                printProgress(k);
            }
        }

        log.warning("Finish loading time:" + (U.currentTimeMillis() - start));

        forceCheckpoint();

        start = U.currentTimeMillis();

        log.warning("Start loading");

        try (IgniteDataStreamer<Object, Object> st = ig0.dataStreamer("cache1")){
            st.allowOverwrite(true);

            for (int k = 0; k < entryCnt; k++){
                st.addData(k, k);

                printProgress(k);
            }
        }

        log.warning("Finish loading time:" + (U.currentTimeMillis() - start));

        forceCheckpoint();

        Lock lock = ig0.cache("cache1").lock(0);

        lock.lock();

        try {
            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        startGrid(initGridCnt);
                    }
                    catch (Exception e) {
                        fail(e.getMessage());
                    }
                }
            });

            boolean reserved = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    for (int g = 0; g < initGridCnt; g++) {
                        IgniteEx ig = grid(g);

                        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)ig.context().cache().context().wal();

                        Object archiver = GridTestUtils.getFieldValue(wal, "archiver");

                        synchronized (archiver) {
                            Map reserved = GridTestUtils.getFieldValue(archiver, "reserved");

                            if (reserved.isEmpty())
                                return false;
                        }
                    }

                    return true;
                }
            }, 10_000);

            assert reserved;
        }
        finally {
            lock.unlock();
        }

        boolean released = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int g = 0; g < initGridCnt; g++) {
                    IgniteEx ig = grid(g);

                    FileWriteAheadLogManager wal = (FileWriteAheadLogManager)ig.context().cache().context().wal();

                    Object archiver = GridTestUtils.getFieldValue(wal, "archiver");

                    synchronized (archiver) {
                        Map reserved = GridTestUtils.getFieldValue(archiver, "reserved");

                        if (!reserved.isEmpty())
                            return false;
                    }
                }

                return true;
            }
        }, 10_000);

        assert released;
    }

    /**
     *
     */
    private void printProgress(int k){
        if (k % 1000 == 0)
            log.warning("Keys -> " + k);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemovesArePreloadedIfHistoryIsAvailable() throws Exception {
        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        int entryCnt = 10_000;

        IgniteEx ig0 = (IgniteEx) startGrids(2);

        ig0.active(true);

        IgniteCache<Integer, Integer> cache = ig0.cache("cache1");

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, k);

        forceCheckpoint();

        stopGrid(1);

        for (int k = 0; k < entryCnt; k += 2)
            cache.remove(k);

        forceCheckpoint();

        IgniteEx ig1 = startGrid(1);

        IgniteCache<Integer, Integer> cache1 = ig1.cache("cache1");

        assertEquals(entryCnt / 2, cache.size());
        assertEquals(entryCnt / 2, cache1.size());

        for (Integer k = 0; k < entryCnt; k++) {
            if (k % 2 == 0) {
                assertTrue("k=" + k, !cache.containsKey(k));
                assertTrue("k=" + k, !cache1.containsKey(k));
            }
            else {
                assertEquals("k=" + k, k, cache.get(k));
                assertEquals("k=" + k, k, cache1.get(k));
            }
        }

        cache.rebalance().get();

        for (int p = 0; p < ig1.affinity("cache1").partitions(); p++) {
            GridDhtLocalPartition p0 = ig0.context().cache().cache("cache1").context().topology().localPartition(p);
            GridDhtLocalPartition p1 = ig1.context().cache().cache("cache1").context().topology().localPartition(p);

            assertTrue(p0.updateCounter() > 0);
            assertEquals(p0.updateCounter(), p1.updateCounter());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeIsClearedIfHistoryIsUnavailable() throws Exception {
        int entryCnt = 10_000;

        IgniteEx ig0 = (IgniteEx) startGrids(2);

        ig0.active(true);

        IgniteCache<Integer, Integer> cache = ig0.cache("cache1");

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, k);

        forceCheckpoint();

        stopGrid(1);

        for (int k = 0; k < entryCnt; k += 2)
            cache.remove(k);

        forceCheckpoint();

        for (Integer k = 0; k < entryCnt; k++) {
            if (k % 2 == 0)
                assertTrue("k=" + k, !cache.containsKey(k));
            else
                assertEquals("k=" + k, k, cache.get(k));
        }

        IgniteEx ig1 = startGrid(1);

        IgniteCache<Integer, Integer> cache1 = ig1.cache("cache1");

        assertEquals(entryCnt / 2, cache.size());
        assertEquals(entryCnt / 2, cache1.size());

        for (Integer k = 0; k < entryCnt; k++) {
            if (k % 2 == 0) {
                assertTrue("k=" + k, !cache.containsKey(k));
                assertTrue("k=" + k, !cache1.containsKey(k));
            }
            else {
                assertEquals("k=" + k, k, cache.get(k));
                assertEquals("k=" + k, k, cache1.get(k));
            }
        }

        cache.rebalance().get();

        for (int p = 0; p < ig1.affinity("cache1").partitions(); p++) {
            GridDhtLocalPartition p0 = ig0.context().cache().cache("cache1").context().topology().localPartition(p);
            GridDhtLocalPartition p1 = ig1.context().cache().cache("cache1").context().topology().localPartition(p);

            assertTrue(p0.updateCounter() > 0);
            assertEquals(p0.updateCounter(), p1.updateCounter());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeLeftDuringExchange() throws Exception {
        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        final int entryCnt = 10_000;
        final int initGridCnt = 4;

        final Ignite ig0 = startGrids(initGridCnt);

        ig0.active(true);

        IgniteCache<Object, Object> cache = ig0.cache("cache1");

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, k);

        forceCheckpoint();

        Lock lock = cache.lock(0);

        lock.lock();

        try {
            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        startGrid(initGridCnt);
                    }
                    catch (Exception e) {
                        fail(e.getMessage());
                    }
                }
            });

            boolean reserved = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    for (int g = 0; g < initGridCnt; g++) {
                        IgniteEx ig = grid(g);

                        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)ig.context().cache().context().wal();

                        Object archiver = GridTestUtils.getFieldValue(wal, "archiver");

                        synchronized (archiver) {
                            Map reserved = GridTestUtils.getFieldValue(archiver, "reserved");

                            if (reserved.isEmpty())
                                return false;
                        }
                    }

                    return true;
                }
            }, 10_000);

            assert reserved;

            stopGrid(Integer.toString(initGridCnt - 1), true, false);
        }
        finally {
            lock.unlock();
        }

        boolean released = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int g = 0; g < initGridCnt - 1; g++) {
                    IgniteEx ig = grid(g);

                    FileWriteAheadLogManager wal = (FileWriteAheadLogManager)ig.context().cache().context().wal();

                    Object archiver = GridTestUtils.getFieldValue(wal, "archiver");

                    synchronized (archiver) {
                        Map reserved = GridTestUtils.getFieldValue(archiver, "reserved");

                        if (!reserved.isEmpty())
                            return false;
                    }
                }

                return true;
            }
        }, 10_000);

        assert released;

        awaitPartitionMapExchange();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     *
     */
    private void forceCheckpoint() throws Exception {
        for (Ignite ignite : G.allGrids()) {
            if (ignite.cluster().localNode().isClient())
                continue;

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                .cache().context().database();

            dbMgr.waitForCheckpoint("test");
        }
    }
}
