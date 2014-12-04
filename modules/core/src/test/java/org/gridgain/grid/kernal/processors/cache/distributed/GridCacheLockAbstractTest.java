/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test cases for multi-threaded tests.
 */
@SuppressWarnings({"FieldCanBeLocal"})
public abstract class GridCacheLockAbstractTest extends GridCommonAbstractTest {
    /** Grid1. */
    private static Ignite ignite1;

    /** Grid2. */
    private static Ignite ignite2;

    /** (for convenience). */
    private static GridCache<Integer, String> cache1;

    /** (for convenience). */
    private static GridCache<Integer, String> cache2;

    /** Ip-finder. */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    protected GridCacheLockAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setWriteSynchronizationMode(FULL_ASYNC);
        cacheCfg.setPreloadMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        return cacheCfg;
    }

    /**
     * @return Cache mode.
     */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);

        cache1 = ignite1.cache(null);
        cache2 = ignite2.cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        info("Executing afterTest() callback...");

        info("Before 1st removeAll().");

        cache1.flagsOn(GridCacheFlag.SYNC_COMMIT).removeAll();

        info("Before 2nd removeAll().");

        cache2.flagsOn(GridCacheFlag.SYNC_COMMIT).removeAll();

        assert cache1.isEmpty() : "Cache is not empty: " + cache1.entrySet();
        assert cache2.isEmpty() : "Cache is not empty: " + cache2.entrySet();
    }

    /**
     * @return Partitioned flag.
     */
    protected boolean isPartitioned() {
        return false;
    }

    /**
     * @param k Key to check.
     * @param idx Grid index.
     * @return {@code True} if locked.
     */
    private boolean locked(Integer k, int idx) {
        if (isPartitioned())
            return near(idx).isLockedNearOnly(k);

        return cache(idx).isLocked(k);
    }

    /**
     * @param keys Keys to check.
     * @param idx Grid index.
     * @return {@code True} if locked.
     */
    private boolean locked(Iterable<Integer> keys, int idx) {
        if (isPartitioned())
            return near(idx).isAllLockedNearOnly(keys);

        for (Integer key : keys) {
            if (!cache(idx).isLocked(key))
                return false;
        }

        return true;
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testLockSingleThread() throws Exception {
        int k = 1;
        String v = String.valueOf(k);

        info("Before lock for key: " + k);

        assert cache1.lock(k, 0L);

        info("After lock for key: " + k);

        try {
            assert cache1.isLocked(k);
            assert cache1.isLockedByThread(k);

            // Put to cache.
            cache1.put(k, v);

            info("Put " + k + '=' + k + " key pair into cache.");
        }
        finally {
            cache1.unlock(k);

            info("Unlocked key: " + k);
        }

        assert !locked(k, 1);
        assert !cache1.isLockedByThread(k);
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testLock() throws Exception {
        final int kv = 1;

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for key: " + kv);

                assert cache1.lock(kv, 0L);

                info("After lock for key: " + kv);

                try {
                    assert cache1.isLocked(kv);
                    assert cache1.isLockedByThread(kv);

                    l1.countDown();

                    info("Let thread2 proceed.");

                    cache1.put(kv, Integer.toString(kv));

                    info("Put " + kv + '=' + Integer.toString(kv) + " key pair into cache.");
                }
                finally {
                    Thread.sleep(1000);

                    cache1.unlockAll(F.asList(kv));

                    info("Unlocked key in thread 1: " + kv);
                }

                l2.await();

                assert !cache1.isLockedByThread(kv);
                assert !locked(kv, 1);

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                l1.await();

                assert cache2.lock(kv, 0L);

                try {
                    String v = cache2.get(kv);

                    assert v != null : "Value is null for key: " + kv;

                    assertEquals(Integer.toString(kv), v);
                }
                finally {
                    cache2.unlockAll(F.asList(kv));

                    info("Unlocked key in thread 2: " + kv);
                }

                assert !locked(kv, 2);
                assert !cache2.isLockedByThread(kv);

                Thread.sleep(1000);

                l2.countDown();

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockAndPut() throws Exception {
        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                assert cache1.lock(1, 0L);

                info("Locked cache key: 1");

                try {
                    assert cache1.isLocked(1);
                    assert cache1.isLockedByThread(1);

                    info("Verified that cache key is locked: 1");

                    cache1.put(1, "1");

                    info("Put key value pair into cache: 1='1'");

                    l1.countDown();

                    info("Released latch1");

                    // Hold lock for a bit.
                    Thread.sleep(50);

                    info("Woke up from sleep.");
                }
                finally {
                    cache1.unlockAll(F.asList(1));

                    info("Unlocked cache key: 1");
                }

                l2.await();

                assert !locked(1, 1);
                assert !cache1.isLockedByThread(1);

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                info("Beginning to await on latch 1");

                l1.await();

                info("Finished awaiting on latch 1");

                assertEquals("1", cache1.get(1));

                info("Retrieved value from cache for key: 1");

                cache1.put(1, "2");

                info("Put key-value pair into cache: 1='2'");

                assertEquals("2", cache1.remove(1));

                l2.countDown();

                info("Removed key from cache: 1");

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testLockTimeoutTwoThreads() throws Exception {
        int keyCnt = 1;

        final Collection<Integer> keys = new ArrayList<>(keyCnt);

        for (int i = 1; i <= keyCnt; i++)
            keys.add(i);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        IgniteFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Before lock for keys.");

                    assert cache1.lockAll(keys, 0);

                    info("After lock for keys.");

                    try {
                        for (Integer key : keys) {
                            assert cache1.isLocked(key);
                            assert cache1.isLockedByThread(key);
                        }

                        l1.countDown();

                        info("Let thread2 proceed.");

                        for (int i : keys) {
                            info("Before put key: " + i);

                            cache1.put(i, Integer.toString(i));

                            if (i % 50 == 0)
                                info("Stored key pairs in cache: " + i);
                        }
                    }
                    finally {
                        l2.await();

                        info("Before unlock keys in thread 1: " + keys);

                        cache1.unlockAll(keys);

                        info("Unlocked entry for keys.");
                    }

                    assert !locked(keys, 1);

                    return null;
                }
            }, 1, "TEST-THREAD-1");

        IgniteFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Waiting for latch1...");

                    try {
                        l1.await();

                        // This call should not acquire the lock since
                        // other thread is holding it.
                        assert !cache1.lockAll(keys, -1);

                        info("Before unlock keys in thread 2: " + keys);

                        cache1.unlockAll(keys);

                        // The keys should still be locked.
                        for (Integer key : keys)
                            assert cache1.isLocked(key);
                    }
                    finally {
                        l2.countDown();
                    }

                    return null;
                }
            }, 1, "TEST-THREAD-2");

        fut1.get();
        fut2.get();
    }
}
