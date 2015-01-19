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

package org.gridgain.grid.kernal.processors.cache.distributed;

import com.google.common.collect.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Test cases for multi-threaded tests.
 */
public abstract class GridCacheMultiNodeLockAbstractTest extends GridCommonAbstractTest {
    /** Grid 1. */
    private static Ignite ignite1;

    /** Grid 2. */
    private static Ignite ignite2;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Listeners. */
    private static Collection<IgnitePredicate<IgniteEvent>> lsnrs = new ArrayList<>();

    /**
     *
     */
    protected GridCacheMultiNodeLockAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /**
     * @return {@code True} for partitioned caches.
     */
    protected boolean partitioned() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);

        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite1 = null;
        ignite2 = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        removeListeners(ignite1);
        removeListeners(ignite2);

        lsnrs.clear();

        for (int i = 1; i <= 3; i++) {
            cache(i).clearAll();

            assertTrue(
                "Cache isn't empty [i=" + i + ", entries=" + ((GridKernal)grid(i)).internalCache().entries() + "]",
                cache(i).isEmpty());
        }
    }

    /**
     * @param ignite Grid to remove listeners from.
     */
    private void removeListeners(Ignite ignite) {
        for (IgnitePredicate<IgniteEvent> lsnr : lsnrs)
            ignite.events().stopLocalListen(lsnr);
    }

    /**
     * @param ignite Grid
     * @param lsnr Listener.
     */
    void addListener(Ignite ignite, IgnitePredicate<IgniteEvent> lsnr) {
        if (!lsnrs.contains(lsnr))
            lsnrs.add(lsnr);

        ignite.events().localListen(lsnr, EVTS_CACHE);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkLocked(GridCacheProjection<Integer,String> cache, Integer key) {
        assert cache.isLocked(key);
        assert cache.isLockedByThread(key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkLocked(IgniteCache<Integer,String> cache, Integer key) {
        assert cache.isLocked(key);
        assert cache.isLockedByThread(key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkRemoteLocked(GridCacheProjection<Integer,String> cache, Integer key) {
        assert cache.isLocked(key);
        assert !cache.isLockedByThread(key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkRemoteLocked(IgniteCache<Integer,String> cache, Integer key) {
        assert cache.isLocked(key);
        assert !cache.isLockedByThread(key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    @SuppressWarnings({"BusyWait"})
    private void checkUnlocked(GridCacheProjection<Integer,String> cache, Integer key) {
        assert !cache.isLockedByThread(key);

        if (partitioned()) {
            for(int i = 0; i < 200; i++)
                if (cache.isLocked(key)) {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else
                    return;
        }

        assertFalse("Key locked [key=" + key + ", entries=" + entries(key) + "]", cache.isLocked(key));
    }
    /**
     * @param cache Cache.
     * @param key Key.
     */
    @SuppressWarnings({"BusyWait"})
    private void checkUnlocked(IgniteCache<Integer,String> cache, Integer key) {
        assert !cache.isLockedByThread(key);

        if (partitioned()) {
            for(int i = 0; i < 200; i++)
                if (cache.isLocked(key)) {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else
                    return;
        }

        assertFalse("Key locked [key=" + key + ", entries=" + entries(key) + "]", cache.isLocked(key));
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkLocked(GridCacheProjection<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys) {
            checkLocked(cache, key);
        }
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkLocked(IgniteCache<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys)
            checkLocked(cache, key);
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkRemoteLocked(GridCacheProjection<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys) {
            checkRemoteLocked(cache, key);
        }
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkRemoteLocked(IgniteCache<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys)
            checkRemoteLocked(cache, key);
    }

    /**
     *
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkUnlocked(GridCacheProjection<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys)
            checkUnlocked(cache, key);
    }

    /**
     *
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkUnlocked(IgniteCache<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys)
            checkUnlocked(cache, key);
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testBasicLock() throws Exception {
        IgniteCache<Integer, String> cache = ignite1.jcache(null);

        cache.lock(1).lock();

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        cache.lockAll(Collections.singleton(1)).unlock();

        checkUnlocked(cache, 1);
    }

    /**
     * Entries for key.
     *
     * @param key Key.
     * @return Entries.
     */
    private String entries(int key) {
        if (partitioned()) {
            GridNearCacheAdapter<Integer, String> near1 = near(1);
            GridNearCacheAdapter<Integer, String> near2 = near(2);

            GridDhtCacheAdapter<Integer, String> dht1 = dht(1);
            GridDhtCacheAdapter<Integer, String> dht2 = dht(2);

            return "Entries [ne1=" + near1.peekEx(key) + ", de1=" + dht1.peekEx(key) + ", ne2=" + near2.peekEx(key) +
                ", de2=" + dht2.peekEx(key) + ']';
        }

        return "Entries [e1=" + ignite1.cache(null).entry(key) + ", e2=" + ignite2.cache(null).entry(key) + ']';
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLock() throws Exception {
        IgniteCache<Integer, String> cache1 = ignite1.jcache(null);
        IgniteCache<Integer, String> cache2 = ignite2.jcache(null);

        cache1.lock(1).lock();

        assert cache1.isLocked(1) : entries(1);
        assert cache1.isLockedByThread(1);

        assert cache2.isLocked(1) : entries(1);
        assert !cache2.isLockedByThread(1);

        try {
            assert !cache2.lock(1).tryLock();

            assert cache2.isLocked(1) : entries(1);
            assert !cache2.isLockedByThread(1);
        }
        finally {
            cache1.lock(1).unlock();

            checkUnlocked(cache1, 1);
        }

        cache2.lock(1).lock();

        assert cache2.isLocked(1) : entries(1);
        assert cache2.isLockedByThread(1);

        assert cache1.isLocked(1) : entries(1);
        assert !cache1.isLockedByThread(1);

        CountDownLatch latch = new CountDownLatch(1);

        addListener(ignite1, new UnlockListener(latch, 1));

        try {
            assert !cache1.lock(1).tryLock();

            assert cache1.isLocked(1) : entries(1);
            assert !cache1.isLockedByThread(1);
        }
        finally {
            cache2.lockAll(Collections.singleton(1)).unlock();
        }

        latch.await();

        checkUnlocked(cache1, 1);
        checkUnlocked(cache2, 1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLockAsync() throws Exception {
        IgniteCache<Integer, String> cache1 = ignite1.jcache(null);
        IgniteCache<Integer, String> cache2 = ignite2.jcache(null);

        CacheLock lock1_1 = cache1.lock(1);
        CacheLock lock2_1 = cache2.lock(1);

        lock1_1.enableAsync().lock();

        assert lock1_1.enableAsync().<Boolean>future().get();

        try {
            assert cache1.isLocked(1);
            assert cache1.isLockedByThread(1);

            assert cache2.isLocked(1);
            assert !cache2.isLockedByThread(1);

            lock2_1.enableAsync().tryLock(-1, TimeUnit.MILLISECONDS);

            assert !lock2_1.enableAsync().<Boolean>future().get();
        }
        finally {
            lock1_1.unlock();
        }

        checkUnlocked(cache1, 1);

        lock2_1.lock();

        CountDownLatch latch = new CountDownLatch(1);

        addListener(ignite1, new UnlockListener(latch, 1));

        try {
            assert cache1.isLocked(1);
            assert !cache1.isLockedByThread(1);

            assert cache2.isLocked(1);
            assert cache2.isLockedByThread(1);

            lock1_1.enableAsync().tryLock(-1, TimeUnit.MILLISECONDS);

            assert !lock1_1.enableAsync().<Boolean>future().get();
        }
        finally {
            lock2_1.unlock();
        }

        latch.await();

        assert !cache1.isLocked(1);
        assert !cache1.isLockedByThread(1);

        checkUnlocked(cache1, 1);
        checkUnlocked(cache2, 1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLockWithKeyLists() throws Exception {
        IgniteCache<Integer, String> cache1 = ignite1.jcache(null);
        IgniteCache<Integer, String> cache2 = ignite2.jcache(null);

        Set<Integer> keys1 = new HashSet<>();
        Set<Integer> keys2 = new HashSet<>();

        Collections.addAll(keys1, 1, 2, 3);
        Collections.addAll(keys2, 2, 3, 4);

        cache1.lockAll(keys1).lock();

        checkLocked(cache1, keys1);

        try {
            assert !cache2.lockAll(keys2).tryLock();

            assert cache2.isLocked(2);
            assert cache2.isLocked(3);

            checkUnlocked(cache1, 4);
            checkUnlocked(cache2, 4);

            assert !cache2.isLockedByThread(2);
            assert !cache2.isLockedByThread(3);
            assert !cache2.isLockedByThread(4);
        }
        finally {
            cache1.lockAll(keys1).unlock();
        }

        checkUnlocked(cache1, keys1);

        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, 4);

        cache2.lockAll(keys2).lock();

        CountDownLatch latch1 = new CountDownLatch(keys2.size());
        CountDownLatch latch2 = new CountDownLatch(1);

        addListener(ignite2, new UnlockListener(latch2, 1));
        addListener(ignite1, (new UnlockListener(latch1, keys2)));

        try {
            checkLocked(cache2, keys2);

            checkUnlocked(cache2, 1);

            assert cache1.lock(1).tryLock();

            checkLocked(cache1, 1);

            checkRemoteLocked(cache1, keys2);

            checkRemoteLocked(cache2, 1);
        }
        finally {
            cache2.lockAll(keys2).unlock();

            cache1.lockAll(Collections.singleton(1)).unlock();
        }

        latch1.await();
        latch2.await();

        checkUnlocked(cache1, keys1);
        checkUnlocked(cache2, keys1);
        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, keys2);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLockAsyncWithKeyLists() throws Exception {
        IgniteCache<Integer, String> cache1 = ignite1.jcache(null);
        IgniteCache<Integer, String> cache2 = ignite2.jcache(null);

        Collection<Integer> keys1 = Lists.newArrayList(1, 2, 3);
        Collection<Integer> keys2 = Lists.newArrayList(2, 3, 4);

        CacheLock lock1_1 = cache1.lockAll(keys1);
        CacheLock lock2_2 = cache2.lockAll(keys2);

        lock1_1.enableAsync().lock();

        IgniteFuture<Boolean> f1 = lock1_1.enableAsync().future();

        try {
            assert f1.get();

            checkLocked(cache1, keys1);

            checkRemoteLocked(cache2, keys1);

            lock2_2.tryLock(-1, TimeUnit.MILLISECONDS);

            IgniteFuture<Boolean> f2 = lock2_2.future();

            assert !f2.get();

            checkLocked(cache1, keys1);

            checkUnlocked(cache1, 4);
            checkUnlocked(cache2, 4);

            checkRemoteLocked(cache2, keys1);
        }
        finally {
            lock1_1.unlock();
        }

        checkUnlocked(cache1, keys1);

        CountDownLatch latch = new CountDownLatch(keys2.size());

        addListener(ignite1, new UnlockListener(latch, keys2));

        lock2_2.enableAsync().lock();

        IgniteFuture<Boolean> f2 = lock2_2.enableAsync().future();

        try {
            assert f2.get();

            checkLocked(cache2, keys2);

            checkRemoteLocked(cache1, keys2);

            checkUnlocked(cache1, 1);

            assert !cache1.lockAll(keys2).tryLock();

            checkLocked(cache2, keys2);

            checkUnlocked(cache1, 1);
            checkUnlocked(cache2 , 1);

            checkRemoteLocked(cache1, keys2);
        }
        finally {
            lock2_2.unlock();
        }

        latch.await();

        checkUnlocked(cache1, keys1);
        checkUnlocked(cache2, keys1);
        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, keys2);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testLockReentry() throws IgniteCheckedException {
        IgniteCache<Integer, String> cache = ignite1.jcache(null);

        cache.lock(1).lock();

        try {
            checkLocked(cache, 1);

            cache.lock(1).lock();

            checkLocked(cache, 1);

            cache.lockAll(Collections.singleton(1)).unlock();

            checkLocked(cache, 1);
        }
        finally {
            cache.lockAll(Collections.singleton(1)).unlock();
        }

        checkUnlocked(cache, 1);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockMultithreaded() throws Exception {
        final IgniteCache<Integer, String> cache = ignite1.jcache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for.key 1");

                cache.lock(1).lock();

                info("After lock for key 1");

                try {
                    checkLocked(cache, 1);

                    l1.countDown();

                    info("Let thread2 proceed.");

                    // Reentry.
                    cache.lock(1).lock();

                    checkLocked(cache, 1);

                    // Nested lock.
                    assert cache.lock(2).tryLock();

                    checkLocked(cache, 2);

                    // Unlock reentry.
                    cache.lockAll(Collections.singleton(1)).unlock();

                    // Outer should still be owned.
                    checkLocked(cache, 1);

                    // Unlock in reverse order.
                    cache.lockAll(Collections.singleton(2)).unlock();

                    checkUnlocked(cache, 2);

                    l2.await();

                    info("Waited for latch 2");
                }
                finally {
                    cache.lockAll(Collections.singleton(1)).unlock();

                    info("Unlocked entry for key 1.");
                }

                assert !cache.isLockedByThread(1);
                assert !cache.isLockedByThread(2);

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                l1.await();

                info("Latch1 released.");

                assert !cache.lock(1).tryLock();

                info("Tried to lock cache for key1");

                l2.countDown();

                info("Released latch2");

                cache.lock(1).lock();

                try {
                    info("Locked cache for key 1");

                    checkLocked(cache, 1);

                    info("Checked that cache is locked for key 1");
                }
                finally {
                    cache.lockAll(Collections.singleton(1)).unlock();

                    info("Unlocked cache for key 1");
                }

                checkUnlocked(cache, 1);

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        checkUnlocked(cache, 1);
        checkUnlocked(cache, 2);
    }

    /**
     * Cache unlock listener.
     */
    private class UnlockListener implements IgnitePredicate<IgniteEvent> {
        /** Latch. */
        private final CountDownLatch latch;

        /** */
        private final Collection<Integer> keys;

        /**
         * @param latch Latch.
         * @param keys Keys.
         */
        UnlockListener(CountDownLatch latch, Integer... keys) {
            this(latch, Arrays.asList(keys));
        }

        /**
         * @param latch Latch.
         * @param keys Keys.
         */
        UnlockListener(CountDownLatch latch, Collection<Integer> keys) {
            assert latch != null;
            assert keys != null;

            this.latch = latch;
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            info("Received cache event: " + evt);

            if (evt instanceof IgniteCacheEvent) {
                IgniteCacheEvent cacheEvt = (IgniteCacheEvent)evt;

                Integer key = cacheEvt.key();

                if (keys.contains(key))
                    if (evt.type() == EVT_CACHE_OBJECT_UNLOCKED)
                        latch.countDown();
            }

            return true;
        }
    }
}
