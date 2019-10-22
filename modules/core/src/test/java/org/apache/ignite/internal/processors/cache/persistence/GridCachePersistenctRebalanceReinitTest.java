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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridCachePreloadSharedManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.io.GridFileUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.util.IgniteUtils.GB;

/**
 *
 */
public class GridCachePersistenctRebalanceReinitTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 8;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC); // todo other sync modes

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dscfg = new DataStorageConfiguration();

        dscfg.setWalMode(WALMode.LOG_ONLY);

        DataRegionConfiguration reg = new DataRegionConfiguration();

        reg.setMaxSize(2 * GB);
        reg.setPersistenceEnabled(true);

        dscfg.setDefaultDataRegionConfiguration(reg);
        dscfg.setCheckpointFrequency(60_000);
        dscfg.setMaxWalArchiveSize(10 * GB);

        cfg.setDataStorageConfiguration(dscfg);

        return cfg;
    }

    /** */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();
    }

    @Test
    @Ignore
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "1")
    public void checkInitPartitionWithConstantLoad() throws Exception {
        fail("Doesn't support classic evictions");

        IgniteEx node0 = startGrid(1);
        IgniteEx node1 = startGrid(2);

        node0.cluster().active(true);
        node0.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        IgniteInternalCache<Integer, Integer> cache = node0.cachex(DEFAULT_CACHE_NAME);

        AtomicLong cntr = new AtomicLong();

        ConstantLoader ldr = new ConstantLoader(node0.cache(DEFAULT_CACHE_NAME), cntr);

        IgniteInternalFuture ldrFut = GridTestUtils.runAsync(ldr);

        U.sleep(1_000);

        forceCheckpoint();

        // Switch to read-only node1
        GridCacheContext<Object, Object> cctx = node1.cachex(DEFAULT_CACHE_NAME).context();

        GridCachePreloadSharedManager preloader = node1.context().cache().context().filePreloader();

        GridCompoundFuture<Void,Void> destroyFut = new GridCompoundFuture<>();

        AffinityTopologyVersion topVer = cctx.topology().readyTopologyVersion();

        // Destroy partitions.
        for (int p : cctx.affinity().backupPartitions(cctx.localNodeId(), topVer)) {
            GridDhtLocalPartition part = cctx.topology().localPartition(p);

            part.moving();

            part.dataStore().store(true).reinit();

            cctx.shared().database().checkpointReadLock();
            try {
                part.dataStore().readOnly(true);
            } finally {
                cctx.shared().database().checkpointReadUnlock();
            }

            // Simulating that part was downloaded and compltely destroying partition.
            part.clearAsync();

            GridFutureAdapter fut = new GridFutureAdapter();

            part.onClearFinished(f -> {
//                ((ReadOnlyGridCacheDataStore)part.dataStore().store(true)).disableRemoves();

                cctx.group().onPartitionEvicted(p);

                try {
                    IgniteInternalFuture<Boolean> fut0 = cctx.group().offheap().destroyCacheDataStore(part.dataStore());

                    ((GridCacheDatabaseSharedManager)cctx.shared().database()).cancelOrWaitPartitionDestroy(cctx.groupId(), p);

                    ((PageMemoryEx)cctx.shared().database().dataRegion(cctx.dataRegion().config().getName()).pageMemory())
                        .clearAsync(
                            (grp, pageId) ->
                                grp == cctx.groupId() && PageIdUtils.partId(pageId) == p, true)
                        .listen(c1 -> {
                            //                        if (log.isDebugEnabled())
                            //                            log.debug("Eviction is done [region=" + name + "]");
                            System.out.println(">>> mem cleared p=" + p);

                            fut.onDone();
                        });
                } catch (Exception e) {
                    fut.onDone(e);
                }

            });

            destroyFut.add(fut);
        }

        destroyFut.markInitialized();

        forceCheckpoint(node1);

        U.sleep(1_000);

        ldr.pause();

        forceCheckpoint(node0);

        Map<Integer, File> partFiles = new HashMap<>();

        for (int p : cctx.affinity().backupPartitions(cctx.localNodeId(), topVer)) {
            GridDhtLocalPartition part = cache.context().topology().localPartition(p);

            File src = new File(filePageStorePath(part));

            String node1filePath = filePageStorePath(cctx.topology().localPartition(part.id()));

            File dest = new File(node1filePath +  ".tmp");

            System.out.println(">> copy " + src + " -> " + dest);

            RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

            GridFileUtils.copy(ioFactory, src, ioFactory, dest, Long.MAX_VALUE);

            partFiles.put(part.id(), dest);
        }

        ldr.resume();

        U.sleep(1_000);

        destroyFut.get();

        Set<Integer> backupParts = cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

        int backupPartsCnt = backupParts.size();

        long[] hwms = new long[backupPartsCnt];
        long[] lwms = new long[backupPartsCnt];
        int[] partsArr = new int[backupPartsCnt];

        IgniteInternalFuture[] futs = new IgniteInternalFuture[backupPartsCnt];

        // Restore partitions.
        int n = 0;

        for (int p : backupParts) {
            GridDhtLocalPartition part = cctx.topology().localPartition(p);

            futs[n++] = preloader.restorePartition(cctx.groupId(), part.id(), partFiles.get(part.id()), null);
        }

        forceCheckpoint(node1);

        n = 0;

        for (int p : backupParts) {
            IgniteInternalFuture fut = futs[n];

            T2<Long, Long> cntrPair = (T2<Long, Long>)fut.get();

            lwms[n] = cntrPair.get1();
            hwms[n] = cntrPair.get2();
            partsArr[n] = p;

            System.out.println(">>>> Triggering rebalancing: part " + p + " [" + lwms[n] + " - " + hwms[n] + "]");

            ++n;
        }

        System.out.println(">>> wait 2 sec");

        U.sleep(2_000);

        System.out.println(">>> STOP");

//        GridTestUtils.setFieldValue(cctx.shared().exchange(), "rebTopVer", cctx.shared().exchange().readyAffinityVersion());
//
//        preloader.triggerHistoricalRebalance(node0.localNode(), cctx, partsArr, lwms, hwms, backupPartsCnt);
//
//        System.out.println("Wait rebalance finish");
//
//        // todo fix topology changes
//        cctx.preloader().rebalanceFuture().get();
//
//        ldr.stop();
//
//        ldrFut.get();
//
//        System.out.println("Validating data");
//
//        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};
//
//        IgniteInternalCache<Object, Object> cache0 = node0.cachex(DEFAULT_CACHE_NAME);
//        IgniteInternalCache<Object, Object> cache1 = node1.cachex(DEFAULT_CACHE_NAME);
//
//        int size0 = cache0.localSize(peekAll);
//        int size1 = cache1.localSize(peekAll);
//
//        assertEquals(size0, size1);
//
//        Iterable<Cache.Entry<Object, Object>> itr0 = cache0.localEntries(peekAll);
//
//        for (Cache.Entry<Object, Object> e : itr0)
//            e.getValue().equals(cache1.get(e.getKey()) == e.getValue());

        System.out.println("Stopping");
    }

    /**
     * @param part Partition.
     * @return Absolute path to partition storage file.
     * @throws IgniteCheckedException If store doesn't exists.
     */
    private String filePageStorePath(GridDhtLocalPartition part) throws IgniteCheckedException {
        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)part.group().shared().pageStore();

        return ((FilePageStore)pageStoreMgr.getStore(part.group().groupId(), part.id())).getFileAbsolutePath();
    }

    /** */
    private static class ConstantLoader implements Runnable {
        /** */
        private final AtomicLong cntr;

        /** */
        private volatile boolean pause;

        /** */
        private volatile boolean paused;

        /** */
        private volatile boolean stop;

        /** */
        private final IgniteCache cache;

        /** */
        private final Set<Integer> rmvKeys = new HashSet<>();

        /** */
        private final Random rnd = ThreadLocalRandom.current();

        /** */
        public ConstantLoader(IgniteCache cache, AtomicLong cntr) {
            this.cache = cache;
            this.cntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stop) {
                if (pause) {
                    if (!paused)
                        paused = true;

                    try {
                        U.sleep(100);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        break;
                    }

                    continue;
                }

                long from = cntr.getAndAdd(100);

                for (long i = from; i < from + 100; i++)
                    cache.put(i, i);

                for (long i = from; i < from + 100; i+=10)
                    cache.remove(i);
            }
        }

        public Set<Integer> rmvKeys() {
            return rmvKeys;
        }

        public void stop() {
            stop = true;
        }

        public void pause() {
            pause = true;

            while (!paused) {
                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void resume() {
            paused = false;
            pause = false;

        }
    }

    @Test
    @Ignore
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "1")
    public void checkInitPartition() throws Exception {
        int initCnt = 5_000 * PARTS_CNT;
        int preloadCnt = initCnt * 2;
        int totalCnt = preloadCnt * 2;

        IgniteEx node0 = startGrid(1);
        IgniteEx node1 = startGrid(2);

        node0.cluster().active(true);
        node0.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        IgniteInternalCache<Integer, Integer> cache = node0.cachex(DEFAULT_CACHE_NAME);

        for (int i = 0; i < initCnt; i++)
            cache.put(i, i);

        forceCheckpoint();

        GridCacheContext<Object, Object> cctx = node1.cachex(DEFAULT_CACHE_NAME).context();

        GridCachePreloadSharedManager preloader = node1.context().cache().context().filePreloader();

        GridCompoundFuture<Void,Void> destroyFut = new GridCompoundFuture<>();

        destroyFut.markInitialized();

        // Destroy partitions.
        for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
            part.moving();

            // Simulating that part was downloaded and compltely destroying partition.
//            destroyFut.add(preloader.schedulePartitionDestroy(part));
        }

        forceCheckpoint(node1);

        for (int i = initCnt; i < preloadCnt; i++)
            cache.put(i, i);

        forceCheckpoint(node0);

        List<GridDhtLocalPartition> parts = cache.context().topology().localPartitions();

        File[] partFiles = new File[parts.size()];

        for (GridDhtLocalPartition part : parts) {
            File src = new File(filePageStorePath(part));

            String node1filePath = filePageStorePath(node1.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(part.id()));

            File dest = new File(node1filePath +  ".tmp");

            System.out.println(">> copy " + src + " -> " + dest);

            RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

            GridFileUtils.copy(ioFactory, src, ioFactory, dest, Long.MAX_VALUE);

            partFiles[part.id()] = dest;
        }

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        assertEquals(preloadCnt, cache.localSize(peekAll));

        // We can re-init partition just after destroy.
        destroyFut.get();

        // Restore partitions.
        for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
            IgniteInternalFuture<T2<Long, Long>> restoreFut =
                preloader.restorePartition(cctx.groupId(), part.id(), partFiles[part.id()], null);

            forceCheckpoint(node1);

            assertTrue(restoreFut.isDone());

            assertEquals("Update counter validation", preloadCnt / PARTS_CNT, (long)restoreFut.get().get2());

            assertTrue(cctx.topology().own(part));

            assertEquals(OWNING, cctx.topology().partitionState(node1.localNode().id(), part.id()));
        }

        for (int i = preloadCnt; i < totalCnt; i++)
            cache.put(i, i);

        for (GridDhtLocalPartition part : cctx.topology().localPartitions())
            assertEquals(totalCnt / cctx.topology().localPartitions().size(), part.fullSize());

        assertEquals(totalCnt, node0.cache(DEFAULT_CACHE_NAME).size());

        for (int i = 0; i < totalCnt; i++)
            assertEquals(String.valueOf(i), i, node0.cachex(DEFAULT_CACHE_NAME).localPeek(i, peekAll));
    }
}
