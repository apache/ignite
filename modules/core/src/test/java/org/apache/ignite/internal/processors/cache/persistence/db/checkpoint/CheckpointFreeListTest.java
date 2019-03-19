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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class CheckpointFreeListTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** Cache name. */
    private static final String CACHE_NAME = "cacheOne";
    /** Cache size */
    public static final int CACHE_SIZE = 30000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setCheckpointThreads(2);
        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(300L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setCacheConfiguration(cacheConfiguration(CACHE_NAME, CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @param mode Cache atomicity mode.
     * @return Configured cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String cacheName, CacheAtomicityMode mode) {
        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(mode)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 1024))
            .setIndexedTypes(String.class, String.class);
    }

    /** Client configuration */
    private IgniteConfiguration getClientConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(true);
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /**
     * @throws Exception if fail.
     */
    @Test
    public void testFreeListRestoredCorrectly() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteEx igniteClient = startGrid(getClientConfiguration("client"));

        Random random = new Random();

        IgniteCache<Integer, Object> cache = igniteClient.cache(CACHE_NAME);

        for (int j = 0; j < CACHE_SIZE; j++) {
            cache.put(j, new byte[random.nextInt(3072)]);

            if (random.nextBoolean())
                cache.remove(j);
        }

        GridCacheOffheapManager offheap = cacheOffheapManager();

        HashMap<Integer, AtomicReferenceArray<PagesList.Stripe[]>> bucketsStorage = new HashMap<>();

        offheap.cacheDataStores().forEach(cacheData ->
            bucketsStorage.put(cacheData.partId(), U.field(cacheData.rowStore().freeList(), "buckets"))
        );

        forceCheckpoint();

        stopGrid(0);

        ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        GridCacheOffheapManager offheap2 = cacheOffheapManager();

        offheap2.cacheDataStores().forEach(cacheData -> {
            AtomicReferenceArray<PagesList.Stripe[]> restoredBuckets = U.field(cacheData.rowStore().freeList(), "buckets");
            AtomicReferenceArray<PagesList.Stripe[]> savedBuckets = bucketsStorage.get(cacheData.partId());

            if (savedBuckets != null && restoredBuckets != null) {
                assertEquals(restoredBuckets.length(), savedBuckets.length());

                for (int i = 0; i < restoredBuckets.length(); i++)
                    assertTrue(Objects.deepEquals(restoredBuckets.get(i), savedBuckets.get(i)));
            }
            else
                assertTrue(savedBuckets == null && restoredBuckets == null);
        });
    }

    /**
     * Extract GridCacheOffheapManager for default cache.
     *
     * @return GridCacheOffheapManager.
     */
    private GridCacheOffheapManager cacheOffheapManager() {
        GridCacheAdapter<Object, Object> cacheInternal = ((IgniteKernal)ignite(0)).internalCache(CACHE_NAME);

        GridCacheAdapter cache0 = (GridCacheAdapter)cacheInternal.cache();

        return (GridCacheOffheapManager)cache0.context().group().offheap();
    }

    /**
     * @throws Exception if fail.
     */
    @Test
    public void testRestoreFreeListCorrectlyAfterRandomStop() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        ignite0.cluster().active(true);

        Random random = new Random();

        List<T2<Integer, byte[]>> cachedEntry = new ArrayList<>();

        IgniteCache<Integer, Object> cache = ignite0.cache(CACHE_NAME);

        for (int j = 0; j < CACHE_SIZE; j++) {
            byte[] val = new byte[random.nextInt(3072)];

            cache.put(j, val);

            cachedEntry.add(new T2<>(j, val));
        }

        Collections.shuffle(cachedEntry);

        //Remove half of entries.
        Collection<T2<Integer, byte[]>> entriesToRemove = cachedEntry.stream().limit(cachedEntry.size() / 2).collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

        entriesToRemove.forEach(t2 -> cache.remove(t2.get1()));

        //During removing of entries free list grab a lot of free pages to itself so will do put/remove again for stabilization of free pages.
        entriesToRemove.forEach(t2 -> cache.put(t2.get1(), t2.get2()));

        entriesToRemove.forEach(t2 -> cache.remove(t2.get1()));

        forceCheckpoint();

        Path cacheFolder = Paths.get(U.defaultWorkDirectory(),
            DFLT_STORE_DIR,
            ignite0.name().replaceAll("\\.", "_"),
            CACHE_DIR_PREFIX + CACHE_NAME
        );

        Optional<Long> totalPartSizeBeforeStop = totalPartitionsSize(cacheFolder);

        CyclicBarrier nodeStartBarrier = new CyclicBarrier(2);

        int approximateIterationCount = 10;

        //Approximate count of entries to put per one iteration.
        int iterationDataCount = entriesToRemove.size() / approximateIterationCount;

        startAsyncPutThread(entriesToRemove, nodeStartBarrier);

        //Will stop node during put data several times.
        while (true) {
            stopGrid(0, true);

            ignite0 = startGrid(0);

            ignite0.cluster().active(true);

            if (entriesToRemove.isEmpty())
                break;

            //Notify put thread that node successfully started.
            nodeStartBarrier.await(20000, TimeUnit.MILLISECONDS);
            nodeStartBarrier.reset();

            int awaitSize = entriesToRemove.size() - iterationDataCount;

            waitForCondition(() -> entriesToRemove.size() < awaitSize || entriesToRemove.size() == 0, 20000);
        }

        forceCheckpoint();

        Optional<Long> totalPartSizeAfterRestore = totalPartitionsSize(cacheFolder);

        //It allow that size after repeated put operations should be not more than on 15%(heuristic value) greater than before operations. In fact, it should not be multiplied in twice every time.
        long correctedRestoreSize = totalPartSizeAfterRestore.get() - (long)(totalPartSizeBeforeStop.get() * 0.15);

        assertTrue("Size after repeated put operations should be not more than on 15% greater. " +
                "Size before = " + totalPartSizeBeforeStop.get() + ", Size after = " + totalPartSizeAfterRestore.get(),
            totalPartSizeBeforeStop.get() > correctedRestoreSize);
    }

    /**
     * @param entriesToPut Entiries to put.
     * @param nodeStartBarrier Marker of node was started.
     */
    private void startAsyncPutThread(Collection<T2<Integer, byte[]>> entriesToPut, CyclicBarrier nodeStartBarrier) {
        GridTestUtils.runAsync(() -> {
            while (true) {
                try {
                    nodeStartBarrier.await(10000, TimeUnit.MILLISECONDS);

                    Ignite ignite = ignite(0);

                    IgniteCache<Integer, Object> cache2 = ignite.cache(CACHE_NAME);

                    Iterator<T2<Integer, byte[]>> iter = entriesToPut.iterator();

                    while (iter.hasNext()) {
                        T2<Integer, byte[]> next = iter.next();

                        cache2.put(next.get1(), next.get2());

                        iter.remove();
                    }
                }
                catch (TimeoutException | InterruptedException | BrokenBarrierException e) {
                    return;
                }
                catch (Exception e) {
                    if (Thread.currentThread().isInterrupted())
                        return;
                }

                if (entriesToPut.isEmpty())
                    break;
            }
        });
    }

    /**
     * @param cacheFolder Folder.
     * @return Total partitinos size.
     */
    private Optional<Long> totalPartitionsSize(Path cacheFolder) {
        return Stream.of(
            requireNonNull(cacheFolder.toFile().listFiles((dir, name) -> name.startsWith(PART_FILE_PREFIX)))
        )
            .map(File::length)
            .reduce(Long::sum);
    }
}
