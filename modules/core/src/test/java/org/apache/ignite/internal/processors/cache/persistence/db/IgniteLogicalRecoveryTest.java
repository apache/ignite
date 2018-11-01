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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.collect.Lists;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * A set of tests that check correctness of logical recovery performed during node start.
 */
public class IgniteLogicalRecoveryTest extends GridCommonAbstractTest {
    /** Shared group name. */
    private static final String SHARED_GROUP_NAME = "group";

    /** Dynamic cache prefix. */
    private static final String DYNAMIC_CACHE_PREFIX = "dynamic-cache-";

    /** Cache prefix. */
    private static final String CACHE_PREFIX = "cache-";

    /** Io factory. */
    private FileIOFactory ioFactory;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration(CACHE_PREFIX + 0, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_PREFIX + 1, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 2, CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_PREFIX + 3, CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 4, SHARED_GROUP_NAME, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 5, SHARED_GROUP_NAME, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL)
        );

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setAlwaysWriteFullPages(true)
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(1024 * 1024 * 1024) // Disable automatic checkpoints.
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setName("dflt")
                    .setInitialSize(256 * 1024 * 1024)
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        if (ioFactory != null)
            dsCfg.setFileIOFactory(ioFactory);

        return cfg;
    }

    /**
     * @param name Name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        return cacheConfiguration(name, null, cacheMode, atomicityMode);
    }

    /**
     * @param name Name.
     * @param groupName Group name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name, @Nullable String groupName, CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        cfg.setGroupName(groupName);
        cfg.setName(name);
        cfg.setCacheMode(cacheMode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setBackups(2);
        cfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.setProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP);
    }

    /**
     *
     */
    public void testRecoveryOnJoinToActiveCluster() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader aggCacheLoader = new AggregateCacheLoader(node);

        aggCacheLoader.start();

        U.sleep(3000);

        forceCheckpoint();

        U.sleep(3000);

        aggCacheLoader.stop();

        stopGrid(2, true);

        startGrid(2);

        awaitPartitionMapExchange();

        aggCacheLoader.consistencyCheck(grid(2));
    }

    /**
     *
     */
    public void testRecoveryOnJoinToInactiveCluster() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader aggCacheLoader = new AggregateCacheLoader(node);

        aggCacheLoader.start();

        U.sleep(3000);

        forceCheckpoint();

        U.sleep(3000);

        aggCacheLoader.stop();

        stopGrid(2, true);

        crd.cluster().active(false);

        startGrid(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        aggCacheLoader.consistencyCheck(grid(2));
    }

    /**
     *
     */
    public void testRecoveryOnDynamicallyStartedCaches() throws Exception {
        List<CacheConfiguration> dynamicCaches = Lists.newArrayList(
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 0, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 1, CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 2, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 3, CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC)
        );

        doTestWithDynamicCaches(dynamicCaches);
    }

    /**
     * @param dynamicCaches Dynamic caches.
     */
    private void doTestWithDynamicCaches(List<CacheConfiguration> dynamicCaches) throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        node.getOrCreateCaches(dynamicCaches);

        AggregateCacheLoader aggCacheLoader = new AggregateCacheLoader(node);

        aggCacheLoader.start();

        U.sleep(3000);

        forceCheckpoint();

        U.sleep(3000);

        aggCacheLoader.stop();

        stopGrid(2, true);

        startGrid(2);

        awaitPartitionMapExchange();

        for (int idx = 0; idx < 3; idx++)
            aggCacheLoader.consistencyCheck(grid(idx));
    }

    /**
     *
     */
    public void testRecoveryOnJoinToDifferentBlt() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader aggCacheLoader = new AggregateCacheLoader(node);

        aggCacheLoader.start();

        U.sleep(3000);

        forceCheckpoint();

        U.sleep(3000);

        aggCacheLoader.stop();

        stopGrid(2, true);

        resetBaselineTopology();

        startGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        for (int idx = 0; idx < 3; idx++)
            aggCacheLoader.consistencyCheck(grid(idx));
    }

    /**
     *
     */
    public void testRecoveryOnCrushDuringCheckpointOnNodeStart() throws Exception {
        // Crash recovery fails because of the bug in pages recycling.
        // Test passes if don't perform removes in cache loader.
        fail("https://issues.apache.org/jira/browse/IGNITE-9303");

        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3, false);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader aggCacheLoader = new AggregateCacheLoader(node);

        aggCacheLoader.start();

        U.sleep(3000);

        forceCheckpoint();

        U.sleep(3000);

        aggCacheLoader.stop();

        stopGrid(2, false);

        ioFactory = new CheckpointFailIoFactory();

        IgniteInternalFuture startNodeFut = GridTestUtils.runAsync(() -> startGrid(2));

        try {
            startNodeFut.get();
        }
        catch (Exception expected) { }

        ioFactory = null;

        // Start node again and check recovery.
        startGrid(2);

        awaitPartitionMapExchange();

        for (int idx = 0; idx < 3; idx++)
            aggCacheLoader.consistencyCheck(grid(idx));
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600 * 1000;
    }

    /**
     *
     */
    private class AggregateCacheLoader {
        /** Ignite. */
        IgniteEx ignite;

        /** Stop flag. */
        AtomicBoolean stopFlag;

        /** Cache loaders. */
        Map<CacheLoader, IgniteInternalFuture> cacheLoaders;

        /**
         * @param ignite Ignite.
         */
        public AggregateCacheLoader(IgniteEx ignite) {
            this.ignite = ignite;
        }

        /**
         *
         */
        public void start() {
            if (stopFlag != null && !stopFlag.get())
                throw new IllegalStateException("Cache loaders must be stopped before start again");

            stopFlag = new AtomicBoolean();
            cacheLoaders = new HashMap<>();

            for (String cacheName : ignite.cacheNames()) {
                CacheLoader loader = new CacheLoader(ignite, stopFlag, cacheName);

                IgniteInternalFuture loadFuture = GridTestUtils.runAsync(loader);

                cacheLoaders.put(loader, loadFuture);
            }
        }

        /**
         *
         */
        public void stop() throws IgniteCheckedException {
            if (stopFlag != null)
                stopFlag.set(true);

            if (cacheLoaders != null)
                for (IgniteInternalFuture loadFuture : cacheLoaders.values())
                    loadFuture.get();
        }

        /**
         * @param ignite Ignite.
         */
        public void consistencyCheck(IgniteEx ignite) throws IgniteCheckedException {
            if (cacheLoaders != null)
                for (CacheLoader cacheLoader : cacheLoaders.keySet())
                    cacheLoader.consistencyCheck(ignite);
        }
    }

    /**
     *
     */
    static class CacheLoader implements Runnable {
        /** Keys space. */
        static final int KEYS_SPACE = 3096;

        /** Ignite. */
        final IgniteEx ignite;

        /** Stop flag. */
        final AtomicBoolean stopFlag;

        /** Cache name. */
        final String cacheName;

        /** Local cache. */
        final Map<Object, Object> locCache = new ConcurrentHashMap<>();

        /**
         * @param ignite Ignite.
         * @param stopFlag Stop flag.
         * @param cacheName Cache name.
         */
        public CacheLoader(IgniteEx ignite, AtomicBoolean stopFlag, String cacheName) {
            this.ignite = ignite;
            this.stopFlag = stopFlag;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stopFlag.get()) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int key = rnd.nextInt(KEYS_SPACE);

                boolean remove = rnd.nextInt(100) <= 20;

                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName);

                try {
                    if (remove) {
                        cache.remove(key);

                        locCache.remove(key);
                    }
                    else {
                        int[] payload = new int[KEYS_SPACE];
                        Arrays.fill(payload, key);

                        cache.put(key, payload);

                        locCache.put(key, payload);
                    }
                }
                catch (Exception ignored) { }
            }
        }

        /**
         *
         */
        public void consistencyCheck(IgniteEx ignite) {
            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName);

            for (int key = 0; key < KEYS_SPACE; key++) {
                int[] expectedValue = (int[]) locCache.get(key);
                int[] actualValue = (int[]) cache.get(key);

                Assert.assertEquals("Consistency check failed for: " + cache.getName() + ", key=" + key,
                    arrayToString(expectedValue), arrayToString(actualValue));
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CacheLoader loader = (CacheLoader) o;

            return Objects.equals(cacheName, loader.cacheName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(cacheName);
        }
    }

    /**
     *
     */
    static class CheckpointFailIoFactory implements FileIOFactory {
        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = new RandomAccessFileIOFactory().create(file, modes);

            if (file.getName().contains("part-"))
                return new FileIODecorator(delegate) {
                    @Override public int write(ByteBuffer srcBuf) throws IOException {
                        throw new IOException("test");
                    }

                    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                        throw new IOException("test");
                    }

                    @Override public int write(byte[] buf, int off, int len) throws IOException {
                        throw new IOException("test");
                    }
                };

            return delegate;
        }
    }

    /**
     * @param arr Array.
     */
    static String arrayToString(int[] arr) {
        if (arr == null)
            return "null";

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < Math.min(arr.length, 10); i++)
            sb.append(i + ",");
        sb.append(']');

        return sb.toString();
    }
}
