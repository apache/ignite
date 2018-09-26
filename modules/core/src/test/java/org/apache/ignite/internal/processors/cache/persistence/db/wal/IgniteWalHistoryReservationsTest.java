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
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

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

        cfg.setConsistentId("NODE$" + gridName.charAt(gridName.length() - 1));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(512 * 1024);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName("cache1");
        ccfg1.setBackups(1);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg1);

        cfg.setFailureDetectionTimeout(20_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        client = false;

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReservedOnExchange() throws Exception {
        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        final int entryCnt = 10_000;
        final int initGridCnt = 4;

        final IgniteEx ig0 = (IgniteEx)startGrids(initGridCnt + 1);

        ig0.active(true);

        stopGrid(initGridCnt);

        Assert.assertEquals(5, ig0.context().state().clusterState().baselineTopology().consistentIds().size());

        long start = U.currentTimeMillis();

        log.warning("Start loading");

        try (IgniteDataStreamer<Object, Object> st = ig0.dataStreamer("cache1")) {
            for (int k = 0; k < entryCnt; k++) {
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

        try (IgniteDataStreamer<Object, Object> st = ig0.dataStreamer("cache1")) {
            st.allowOverwrite(true);

            for (int k = 0; k < entryCnt; k++) {
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

                        if (isReserveListEmpty(ig))
                            return false;
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

                    if (isReserveListEmpty(ig))
                        return false;
                }

                return true;
            }
        }, 10_000);

        assert released;
    }

    /**
     * @return {@code true} if reserve list is empty.
     */
    private boolean isReserveListEmpty(IgniteEx ig) {
        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)ig.context().cache().context().wal();

        Object segmentAware = GridTestUtils.getFieldValue(wal, "segmentAware");

        synchronized (segmentAware) {
            Map reserved = GridTestUtils.getFieldValue(GridTestUtils.getFieldValue(segmentAware, "reservationStorage"), "reserved");

            if (reserved.isEmpty())
                return true;
        }
        return false;
    }

    /**
     *
     */
    private void printProgress(int k) {
        if (k % 1000 == 0)
            log.warning("Keys -> " + k);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemovesArePreloadedIfHistoryIsAvailable() throws Exception {
        int entryCnt = 10_000;

        IgniteEx ig0 = (IgniteEx)startGrids(2);

        ig0.active(true);

        IgniteCache<Integer, Integer> cache = ig0.cache("cache1");

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, k);

        stopGrid(1);

        for (int k = 0; k < entryCnt; k += 2)
            cache.remove(k);

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

        IgniteEx ig0 = (IgniteEx)startGrids(2);

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
    public void testWalHistoryPartiallyRemoved() throws Exception {
        int entryCnt = 10_000;

        IgniteEx ig0 = (IgniteEx)startGrids(2);

        ig0.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig0.cache("cache1");

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, k);

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                forceCheckpoint();

                return null;
            }
        });

        String nodeId0 = U.maskForFileName(ig0.localNode().consistentId().toString());

        String walArchPath = ig0.configuration().getDataStorageConfiguration().getWalArchivePath();

        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), walArchPath + "/" +
            nodeId0, false));

        startGrid(0);

        Ignite ig1 = startGrid(1);

        ig1.cluster().active(true);
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

                        if (isReserveListEmpty(ig))
                            return false;
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

                    if (isReserveListEmpty(ig))
                        return false;
                }

                return true;
            }
        }, 10_000);

        assert released;

        awaitPartitionMapExchange();
    }
}
