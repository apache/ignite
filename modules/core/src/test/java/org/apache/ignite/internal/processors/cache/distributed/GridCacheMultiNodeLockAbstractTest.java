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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Test cases for multi-threaded tests.
 */
public abstract class GridCacheMultiNodeLockAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE1 = null;

    /** */
    private static final String CACHE2 = "cache2";

    /** Grid 1. */
    private static Ignite ignite1;

    /** Grid 2. */
    private static Ignite ignite2;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Listeners. */
    private static Collection<IgnitePredicate<Event>> lsnrs = new ArrayList<>();

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

        CacheConfiguration ccfg1 = cacheConfiguration().setName(CACHE1);
        CacheConfiguration ccfg2 = cacheConfiguration().setName(CACHE2);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration();

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
            jcache(i).clear();

            assertTrue(
                "Cache isn't empty [i=" + i + ", entries=" + ((IgniteKernal)grid(i)).internalCache().entries() + "]",
                jcache(i).localSize() == 0);
        }
    }

    /**
     * @param ignite Grid to remove listeners from.
     */
    private void removeListeners(Ignite ignite) {
        for (IgnitePredicate<Event> lsnr : lsnrs)
            ignite.events().stopLocalListen(lsnr);
    }

    /**
     * @param ignite Grid
     * @param lsnr Listener.
     */
    void addListener(Ignite ignite, IgnitePredicate<Event> lsnr) {
        if (!lsnrs.contains(lsnr))
            lsnrs.add(lsnr);

        ignite.events().localListen(lsnr, EVTS_CACHE);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkLocked(IgniteCache<Integer,String> cache, Integer key) {
        assert cache.isLocalLocked(key, false);
        assert cache.isLocalLocked(key, true);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkRemoteLocked(IgniteCache<Integer,String> cache, Integer key) {
        assert cache.isLocalLocked(key, false);
        assert !cache.isLocalLocked(key, true);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"BusyWait"})
    private void checkUnlocked(IgniteCache<Integer,String> cache, Integer key) throws IgniteCheckedException {
        assert !cache.isLocalLocked(key, true);

        if (partitioned()) {
            for(int i = 0; i < 200; i++)
                if (cache.isLocalLocked(key, false)) {
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

        assertFalse("Key locked [key=" + key + ", entries=" + entries(key) + "]", cache.isLocalLocked(key, false));
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
    private void checkRemoteLocked(IgniteCache<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys)
            checkRemoteLocked(cache, key);
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     * @throws IgniteCheckedException If failed.
     */
    private void checkUnlocked(IgniteCache<Integer,String> cache, Iterable<Integer> keys) throws IgniteCheckedException {
        for (Integer key : keys)
            checkUnlocked(cache, key);
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testBasicLock() throws Exception {
        IgniteCache<Integer, String> cache = ignite1.cache(null);

        Lock lock = cache.lock(1);

        lock.lock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(1, true);

        lock.unlock();

        checkUnlocked(cache, 1);
    }

    /**
     * Entries for key.
     *
     * @param key Key.
     * @return Entries.
     * @throws IgniteCheckedException If failed.
     */
    private String entries(int key) throws IgniteCheckedException {
        if (partitioned()) {
            GridNearCacheAdapter<Integer, String> near1 = near(1);
            GridNearCacheAdapter<Integer, String> near2 = near(2);

            GridDhtCacheAdapter<Integer, String> dht1 = dht(1);
            GridDhtCacheAdapter<Integer, String> dht2 = dht(2);

            return "Entries [ne1=" + near1.peekEx(key) + ", de1=" + dht1.peekEx(key) + ", ne2=" + near2.peekEx(key) +
                ", de2=" + dht2.peekEx(key) + ']';
        }

        return "Entries [e1=" + "(" + key + ", " + ((IgniteKernal)ignite1).internalCache(null).get(key) + ")"
            + ", e2=" + "(" + key + ", " + ((IgniteKernal)ignite2).internalCache(null).get(key) + ")" + ']';
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLock() throws Exception {
        IgniteCache<Integer, String> cache1 = ignite1.cache(null);
        IgniteCache<Integer, String> cache2 = ignite2.cache(null);

        Lock lock1_1 = cache1.lock(1);
        Lock lock2_1 = cache2.lock(1);

        lock1_1.lock();

        try {
            assert cache1.isLocalLocked(1, false) : entries(1);
            assert cache1.isLocalLocked(1, true);

            assert cache2.isLocalLocked(1, false) : entries(1);
            assert !cache2.isLocalLocked(1, true);

            assert !lock2_1.tryLock();

            assert cache2.isLocalLocked(1, false) : entries(1);
            assert !cache2.isLocalLocked(1, true);
        }
        finally {
            lock1_1.unlock();

            checkUnlocked(cache1, 1);
        }

        CountDownLatch latch = new CountDownLatch(1);

        lock2_1.lock();

        try {
            assert cache2.isLocalLocked(1, false) : entries(1);
            assert cache2.isLocalLocked(1, true);

            assert cache1.isLocalLocked(1, false) : entries(1);
            assert !cache1.isLocalLocked(1, true);

            addListener(ignite1, new UnlockListener(latch, 1));

            assert !lock1_1.tryLock();

            assert cache1.isLocalLocked(1, false) : entries(1);
            assert !cache1.isLocalLocked(1, true);
        }
        finally {
            lock2_1.unlock();
        }

        latch.await();

        checkUnlocked(cache1, 1);
        checkUnlocked(cache2, 1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLockWithKeyLists() throws Exception {
        IgniteCache<Integer, String> cache1 = ignite1.cache(null);
        IgniteCache<Integer, String> cache2 = ignite2.cache(null);

        Collection<Integer> keys1 = Arrays.asList(1, 2, 3);
        Collection<Integer> keys2 = Arrays.asList(2, 3, 4);

        Lock lock1_1 = cache1.lockAll(keys1);
        Lock lock2_2 = cache2.lockAll(keys2);

        lock1_1.lock();

        checkLocked(cache1, keys1);

        try {
            assert !lock2_2.tryLock();

            assert cache2.isLocalLocked(2, false);
            assert cache2.isLocalLocked(3, false);

            checkUnlocked(cache1, 4);
            checkUnlocked(cache2, 4);

            assert !cache2.isLocalLocked(2, true);
            assert !cache2.isLocalLocked(3, true);
            assert !cache2.isLocalLocked(4, true);
        }
        finally {
            lock1_1.unlock();
        }

        checkUnlocked(cache1, keys1);

        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, 4);

        lock2_2.lock();

        CountDownLatch latch1 = new CountDownLatch(keys2.size());
        CountDownLatch latch2 = new CountDownLatch(1);

        addListener(ignite2, new UnlockListener(latch2, 1));
        addListener(ignite1, (new UnlockListener(latch1, keys2)));

        Lock lock1_ = cache1.lock(1);

        try {
            checkLocked(cache2, keys2);

            checkUnlocked(cache2, 1);

            assert lock1_.tryLock();

            checkLocked(cache1, 1);

            checkRemoteLocked(cache1, keys2);

            checkRemoteLocked(cache2, 1);
        }
        finally {
            lock2_2.unlock();

            lock1_.unlock();
        }

        latch1.await();
        latch2.await();

        checkUnlocked(cache1, keys1);
        checkUnlocked(cache2, keys1);
        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, keys2);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testLockReentry() throws IgniteCheckedException {
        IgniteCache<Integer, String> cache = ignite1.cache(null);

        Lock lock = cache.lock(1);

        lock.lock();

        try {
            checkLocked(cache, 1);

            lock.lock();

            checkLocked(cache, 1);

            lock.unlock();

            checkLocked(cache, 1);
        }
        finally {
            lock.unlock();
        }

        checkUnlocked(cache, 1);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockMultithreaded() throws Exception {
        final IgniteCache<Integer, String> cache = ignite1.cache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        final Lock lock1 = cache.lock(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for.key 1");

                lock1.lock();

                info("After lock for key 1");

                try {
                    checkLocked(cache, 1);

                    l1.countDown();

                    info("Let thread2 proceed.");

                    // Reentry.
                    lock1.lock();

                    checkLocked(cache, 1);

                    // Nested lock.
                    Lock lock2 = cache.lock(2);

                    assert lock2.tryLock();

                    checkLocked(cache, 2);

                    // Unlock reentry.
                    lock1.unlock();

                    // Outer should still be owned.
                    checkLocked(cache, 1);

                    // Unlock in reverse order.
                    lock2.unlock();

                    checkUnlocked(cache, 2);

                    l2.await();

                    info("Waited for latch 2");
                }
                finally {
                    lock1.unlock();

                    info("Unlocked entry for key 1.");
                }

                assert !cache.isLocalLocked(1, true);
                assert !cache.isLocalLocked(2, true);

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                l1.await();

                info("Latch1 released.");

                assert !lock1.tryLock();

                info("Tried to lock cache for key1");

                l2.countDown();

                info("Released latch2");

                lock1.lock();

                try {
                    info("Locked cache for key 1");

                    checkLocked(cache, 1);

                    info("Checked that cache is locked for key 1");
                }
                finally {
                    lock1.unlock();

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
     * @throws Exception If failed.
     */
    public void testTwoCaches() throws Exception {
        IgniteCache<Integer, String> cache1 = ignite1.cache(CACHE1);
        IgniteCache<Integer, String> cache2 = ignite1.cache(CACHE2);

        final Integer key = primaryKey(cache1);

        Lock lock = cache1.lock(key);

        lock.lock();

        try {
            assertTrue(cache1.isLocalLocked(key, true));
            assertTrue(cache1.isLocalLocked(key, false));

            assertFalse(cache2.isLocalLocked(key, true));
            assertFalse(cache2.isLocalLocked(key, false));
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Cache unlock listener.
     */
    private class UnlockListener implements IgnitePredicate<Event> {
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
        @Override public boolean apply(Event evt) {
            info("Received cache event: " + evt);

            if (evt instanceof CacheEvent) {
                CacheEvent cacheEvt = (CacheEvent)evt;

                Integer key = cacheEvt.key();

                if (keys.contains(key))
                    if (evt.type() == EVT_CACHE_OBJECT_UNLOCKED)
                        latch.countDown();
            }

            return true;
        }
    }
}
