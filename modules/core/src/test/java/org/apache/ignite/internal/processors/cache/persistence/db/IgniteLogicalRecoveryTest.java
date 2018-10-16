package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

public class IgniteLogicalRecoveryTest extends GridCommonAbstractTest {

    private static final int CACHES = 9;

    private static final String SHARED_GROUP_NAME = "group";

    private static final String DYNAMIC_CACHE_PREFIX = "dynamic-";

    private static final String CACHE_PREFIX = "cache-";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration(CACHE_PREFIX + 0, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_PREFIX + 1, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 2, CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_PREFIX + 3, CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 4, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 5, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC),
            cacheConfigurationSharedGroup(CACHE_PREFIX + 6, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfigurationSharedGroup(CACHE_PREFIX + 7, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfigurationSharedGroup(CACHE_PREFIX + 8, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL)
        );

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.FSYNC)
            .setCheckpointFrequency(1024 * 1024 * 1024) // Disable automatic checkpoints.
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setName("dflt")
                    .setInitialSize(512 * 1024 * 1024)
                    .setMaxSize(2048L * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    private CacheConfiguration<Object, Object> cacheConfiguration(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        cfg.setName(name);
        cfg.setCacheMode(cacheMode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setBackups(2);
        cfg.setAffinity(new RendezvousAffinityFunction(true, 32));

        return cfg;
    }

    private CacheConfiguration<Object, Object> cacheConfigurationSharedGroup(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        cfg.setGroupName(SHARED_GROUP_NAME);
        cfg.setName(name);
        cfg.setCacheMode(cacheMode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setBackups(2);
        cfg.setAffinity(new RendezvousAffinityFunction(true, 32));

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    private List<String> allCaches() {
        return IntStream.range(0, CACHES)
                .mapToObj(i -> CACHE_PREFIX + i)
                .collect(Collectors.toList());
    }

    public void testRecoveryOnJoinToActiveCluster() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader aggCacheLoader = new AggregateCacheLoader(node, allCaches());

        aggCacheLoader.start();

        U.sleep(2500);

        forceCheckpoint();

        U.sleep(2500);

        aggCacheLoader.stop();

        stopGrid(2, true);

        startGrid(2);

        awaitPartitionMapExchange();

        aggCacheLoader.consistencyCheck(grid(2));
    }

    public void testRecoveryOnJoinToInactiveCluster() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader aggCacheLoader = new AggregateCacheLoader(node, allCaches());

        aggCacheLoader.start();

        U.sleep(5000);

        aggCacheLoader.stop();

        stopGrid(2, true);

        crd.cluster().active(false);

        startGrid(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        aggCacheLoader.consistencyCheck(grid(2));
    }

    public void testRecoveryOnActivate() {

    }

    public void testRecoveryOnJoinToDifferentBlt() {

    }

    public void testRecoveryOnDynamicallyStartedCaches() {

    }

    private class AggregateCacheLoader {
        IgniteEx ignite;

        List<String> cacheNames;

        AtomicBoolean stopFlag;

        Map<CacheLoader, IgniteInternalFuture> cacheLoaders;

        public AggregateCacheLoader(IgniteEx ignite, List<String> cacheNames) {
            this.ignite = ignite;
            this.cacheNames = cacheNames;
        }

        public void start() {
            if (stopFlag != null && !stopFlag.get())
                throw new IllegalStateException("Cache loaders must be stopped before start again");

            stopFlag = new AtomicBoolean();
            cacheLoaders = new HashMap<>();

            for (String cacheName : cacheNames) {
                CacheLoader loader = new CacheLoader(ignite, stopFlag, cacheName);

                IgniteInternalFuture loadFuture = GridTestUtils.runAsync(loader);

                cacheLoaders.put(loader, loadFuture);
            }
        }

        public void stop() throws IgniteCheckedException {
            if (stopFlag != null)
                stopFlag.set(true);

            if (cacheLoaders != null)
                for (IgniteInternalFuture loadFuture : cacheLoaders.values())
                    loadFuture.get();
        }

        public void consistencyCheck(IgniteEx ignite) throws IgniteCheckedException {
            if (cacheLoaders != null)
                for (CacheLoader cacheLoader : cacheLoaders.keySet())
                    cacheLoader.consistencyCheck(ignite);
        }
    }

    class CacheLoader implements Runnable {
        static final int KEYS_SPACE = 3096;

        final IgniteEx ignite;

        final AtomicBoolean stopFlag;

        final String cacheName;

        final Map<Object, Object> locCache = new ConcurrentHashMap<>();

        public CacheLoader(IgniteEx ignite, AtomicBoolean stopFlag, String cacheName) {
            this.ignite = ignite;
            this.stopFlag = stopFlag;
            this.cacheName = cacheName;
        }

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

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CacheLoader loader = (CacheLoader) o;

            return Objects.equals(cacheName, loader.cacheName);
        }

        @Override public int hashCode() {
            return Objects.hash(cacheName);
        }
    }

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
