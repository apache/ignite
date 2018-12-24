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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;

/**
 *
 */
@RunWith(JUnit4.class)
public class CheckpointFreeListTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** Cache name. */
    private static final String CACHE_NAME = "cacheOne";
    /** Cache size */
    public static final int CACHE_SIZE = 100000;

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
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
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

        for (int j = 0; j < CACHE_SIZE; j++)
            cache.put(j, new byte[random.nextInt(3072)]);

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
                for (int i = 0; i < restoredBuckets.length(); i++) {
                    PagesList.Stripe[] restoredStripes = restoredBuckets.get(i);
                    PagesList.Stripe[] savedStripes = savedBuckets.get(i);

                    if (savedStripes != null && restoredStripes != null) {
                        assertEquals(restoredStripes.length, savedStripes.length);

                        for (int j = 0; j < savedStripes.length; j++) {
                            if (savedStripes[j] != null && restoredStripes[j] != null)
                                assertEquals(restoredStripes[j].tailId, savedStripes[j].tailId);
                            else
                                assertTrue(savedStripes[j] == null && restoredStripes[j] == null);
                        }
                    }
                    else
                        assertTrue(savedStripes == null && restoredStripes == null);
                }
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
    public void testRestorFreeListCorrectlyAfterRandomStop() throws Exception {
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
        List<T2<Integer, byte[]>> removedEntry = cachedEntry.stream().limit(cachedEntry.size() / 2).collect(Collectors.toList());

        removedEntry.forEach(t2 -> cache.remove(t2.get1()));

        //During removing of entries free list grab a lot of free pages to itself so do put/remove again for stabilization of free pages.
        removedEntry.forEach(t2 -> cache.put(t2.get1(), t2.get2()));

        removedEntry.forEach(t2 -> cache.remove(t2.get1()));

        forceCheckpoint();

        Path cacheFolder = Paths.get(U.defaultWorkDirectory(),
            DFLT_STORE_DIR,
            ignite0.name().replaceAll("\\.", "_"),
            CACHE_DIR_PREFIX + CACHE_NAME
        );

        Optional<Long> totalPartSizeBeforeStop = Stream.of(
            requireNonNull(cacheFolder.toFile().listFiles((dir, name) -> name.startsWith(PART_FILE_PREFIX)))
        )
            .map(File::length)
            .reduce(Long::sum);

        CyclicBarrier nodeStartBarrier = new CyclicBarrier(2);

        IgniteInternalFuture putDataThread = GridTestUtils.runAsync(() -> {
            while (true) {
                try {
                    nodeStartBarrier.await();

                    Ignite ignite = ignite(0);

                    IgniteCache<Integer, Object> cache2 = ignite.cache(CACHE_NAME);

                    Iterator<T2<Integer, byte[]>> iter = removedEntry.iterator();
                    while (iter.hasNext()) {
                        T2<Integer, byte[]> next = iter.next();

                        cache2.put(next.get1(), next.get2());

                        iter.remove();
                    }
                }
                catch (Exception e) {
                    if (Thread.currentThread().isInterrupted())
                        return;
                }

                if (removedEntry.isEmpty())
                    break;
            }
        });

        while (true) {
            stopGrid(0, true);

            startGrid(0);

            nodeStartBarrier.await();

            nodeStartBarrier.reset();

            try {
                //Given some time for load data.
                putDataThread.get(300);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                //No op.
            }
        }

        forceCheckpoint();

        Optional<Long> totalPartSizeAfterRestore = Stream.of(
            requireNonNull(cacheFolder.toFile().listFiles((dir, name) -> name.startsWith(PART_FILE_PREFIX)))
        )
            .map(File::length)
            .reduce(Long::sum);

        //Allowed that size after repeated put operations should be not more than on 5%(heuristic value) greater than before operation.
        long correctedRestoreSize = totalPartSizeAfterRestore.get() - (long)(totalPartSizeBeforeStop.get() * 0.05);

        assertTrue("Size after repeated put operations should be not more than on 5% greater. " +
                "Size before = " + totalPartSizeBeforeStop.get() + ", Size after = " + totalPartSizeAfterRestore.get(),
            totalPartSizeBeforeStop.get() > correctedRestoreSize);
    }

    @Test
    public void testRestorFreeListCorrectlyAfterRandomStop2() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        ignite0.cluster().active(true);

        Random random = new Random();

        List<T2<Integer, byte[]>> cachedEntry = new ArrayList<>();

        IgniteCache<Integer, Object> cache = ignite0.cache(CACHE_NAME);

        for (int j = 0; j < CACHE_SIZE; j++) {
            byte[] val = new byte[random.nextInt(3072)];

            cache.put(j, val);

//            cachedEntry.add(new T2<>(j, val));
        }
    }
}
