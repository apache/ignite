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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestThread;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVTS_CACHE;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * Test cases for multi-threaded tests.
 */
@SuppressWarnings("LockAcquiredButNotSafelyReleased")
public abstract class GridCacheBasicApiAbstractTest extends GridCommonAbstractTest {
    /** Grid. */
    private Ignite ignite;

    /**
     *
     */
    protected GridCacheBasicApiAbstractTest() {
        super(true /*start grid.*/);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testBasicLock() throws Exception {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        Lock lock = cache.lock(1);

        assert lock.tryLock();

        assert cache.isLocalLocked(1, false);

        lock.unlock();

        assert !cache.isLocalLocked(1, false);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testSingleLockReentry() throws IgniteCheckedException {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        Lock lock = cache.lock(1);

        lock.lock();

        try {
            assert cache.isLocalLocked(1, true);

            lock.lock();

            lock.unlock();

            assert cache.isLocalLocked(1, true);
        }
        finally {
            lock.unlock();
        }

        assert !cache.isLocalLocked(1, true);
        assert !cache.isLocalLocked(1, false);
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testReentry() throws Exception {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        Lock lock = cache.lock(1);

        lock.lock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(1, true);

        lock.lock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(1, true);

        lock.lock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(1, true);

        lock.unlock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(1, true);

        lock.unlock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(1, true);

        lock.unlock();

        assert !cache.isLocalLocked(1, false);
        assert !cache.isLocalLocked(1, true);
    }

    /**
     *
     */
    public void testInterruptLock() throws InterruptedException {
        final IgniteCache<Integer, String> cache = ignite.cache(null);

        final Lock lock = cache.lock(1);

        lock.lock();

        final AtomicBoolean isOk = new AtomicBoolean(false);

        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                assertFalse(cache.isLocalLocked(1, true));

                lock.lock();

                try {
                    assertTrue(cache.isLocalLocked(1, true));
                }
                finally {
                    lock.unlock();
                }

                assertTrue(Thread.currentThread().isInterrupted());

                isOk.set(true);
            }
        });

        t.start();

        Thread.sleep(100);

        t.interrupt();

        lock.unlock();

        t.join();

        assertTrue(isOk.get());
    }

    /**
     *
     */
    public void testInterruptLockWithTimeout() throws Exception {
        final IgniteCache<Integer, String> cache = ignite.cache(null);

        startGrid(1);

        try {
            final List<Integer> keys = primaryKeys(grid(1).cache(null), 2, 1);

            Lock lock1 = cache.lock(keys.get(1));

            lock1.lock();

            final AtomicBoolean isOk = new AtomicBoolean(false);

            final CountDownLatch latch = new CountDownLatch(1);

            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        latch.countDown();

                        isOk.set(!cache.lockAll(Arrays.asList(keys.get(0), keys.get(1))).tryLock(5000, MILLISECONDS));
                    }
                    catch (InterruptedException ignored) {
                        isOk.set(false);
                    }
                }
            });

            t.start();

            latch.await();

            Thread.sleep(300);

            t.interrupt();

            t.join();

            lock1.unlock();

            Thread.sleep(1000);

            assertFalse(cache.isLocalLocked(keys.get(0), false));
            assertFalse(cache.isLocalLocked(keys.get(1), false));

            assertFalse(grid(1).cache(null).isLocalLocked(keys.get(0), false));
            assertFalse(grid(1).cache(null).isLocalLocked(keys.get(1), false));

            assertTrue(isOk.get());
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testManyLockReentries() throws IgniteCheckedException {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        Integer key = 1;

        Lock lock = cache.lock(key);

        lock.lock();

        try {
            assert cache.get(key) == null;
            assert cache.getAndPut(key, "1") == null;
            assert "1".equals(cache.get(key));

            assert cache.isLocalLocked(key, false);
            assert cache.isLocalLocked(key, true);

            lock.lock();

            assert cache.isLocalLocked(key, false);
            assert cache.isLocalLocked(key, true);

            try {
                assert "1".equals(cache.getAndRemove(key));
            }
            finally {
                lock.unlock();
            }

            assert cache.isLocalLocked(key, false);
            assert cache.isLocalLocked(key, true);
        }
        finally {
            lock.unlock();

            assert !cache.isLocalLocked(key, false);
            assert !cache.isLocalLocked(key, true);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockMultithreaded() throws Exception {
        final IgniteCache<Integer, String> cache = ignite.cache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        final CountDownLatch l3 = new CountDownLatch(1);

        final Lock lock = cache.lock(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for.key 1");

                lock.lock();

                info("After lock for key 1");

                try {
                    assert cache.isLocalLocked(1, false);
                    assert cache.isLocalLocked(1, true);

                    l1.countDown();

                    info("Let thread2 proceed.");

                    // Reentry.
                    assert lock.tryLock();

                    // Nested lock.
                    Lock lock2 = cache.lock(2);

                    assert lock2.tryLock();

                    l2.await();

                    lock.unlock();

                    // Unlock in reverse order.
                    lock2.unlock();

                    info("Waited for latch 2");
                }
                finally {
                    lock.unlock();

                    info("Unlocked entry for key 1.");
                }

                l3.countDown();

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                l1.await();

                info("Latch1 released.");

                assert !lock.tryLock();

                if (!cache.isLocalLocked(1, false))
                    throw new IllegalArgumentException();

                assert !cache.isLocalLocked(1, true);

                info("Tried to lock cache for key1");

                l2.countDown();

                info("Released latch2");

                l3.await();

                assert lock.tryLock();

                try {
                    info("Locked cache for key 1");

                    assert cache.isLocalLocked(1, false);
                    assert cache.isLocalLocked(1, true);

                    info("Checked that cache is locked for key 1");
                }
                finally {
                    lock.unlock();

                    info("Unlocked cache for key 1");
                }

                assert !cache.isLocalLocked(1, false);
                assert !cache.isLocalLocked(1, true);

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        assert !cache.isLocalLocked(1, false);
    }

    /**
     *
     * @throws Exception If error occur.
     */
    public void testBasicOps() throws Exception {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        CountDownLatch latch = new CountDownLatch(1);

        CacheEventListener lsnr = new CacheEventListener(latch);

        try {
            ignite.events().localListen(lsnr, EVTS_CACHE);

            int key = (int)System.currentTimeMillis();

            assert !cache.containsKey(key);

            cache.put(key, "a");

            info("Start latch wait 1");

            latch.await();

            info("Stop latch wait 1");

            assert cache.containsKey(key);

            latch = new CountDownLatch(2);

            lsnr.latch(latch);

            cache.put(key, "b");
            cache.put(key, "c");

            info("Start latch wait 2");

            latch.await();

            info("Stop latch wait 2");

            assert cache.containsKey(key);

            latch = new CountDownLatch(1);

            lsnr.latch(latch);

            cache.remove(key);

            info("Start latch wait 3");

            latch.await();

            info("Stop latch wait 3");

            assert !cache.containsKey(key);
        }
        finally {
            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }
    }

    /**
     * @throws Exception If error occur.
     */
    public void testBasicOpsWithReentry() throws Exception {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        int key = (int)System.currentTimeMillis();

        assert !cache.containsKey(key);

        Lock lock = cache.lock(key);

        lock.lock();

        CountDownLatch latch = new CountDownLatch(1);

        CacheEventListener lsnr = new CacheEventListener(latch);

        try {
            ignite.events().localListen(lsnr, EVTS_CACHE);

            cache.put(key, "a");

            info("Start latch wait 1");

            latch.await();

            info("Stop latch wait 1");

            assert cache.containsKey(key);
            assert cache.isLocalLocked(key, true);

            latch = new CountDownLatch(2);

            lsnr.latch(latch);

            cache.put(key, "b");
            cache.put(key, "c");

            info("Start latch wait 2");

            latch.await();

            info("Stop latch wait 2");

            assert cache.containsKey(key);
            assert cache.isLocalLocked(key, true);

            latch = new CountDownLatch(1);

            lsnr.latch(latch);

            cache.remove(key);

            info("Start latch wait 3");

            latch.await();

            info("Stop latch wait 3");

            assert cache.isLocalLocked(key, false);
        }
        finally {
            lock.unlock();

            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }

        // Entry should be evicted since allowEmptyEntries is false.
        assert !cache.isLocalLocked(key, false);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultiLocks() throws Exception {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        Collection<Integer> keys = Arrays.asList(1, 2, 3);

        Lock lock = cache.lockAll(keys);

        lock.lock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(2, false);
        assert cache.isLocalLocked(3, false);

        assert cache.isLocalLocked(1, true);
        assert cache.isLocalLocked(2, true);
        assert cache.isLocalLocked(3, true);

        lock.unlock();

        assert !cache.isLocalLocked(1, false);
        assert !cache.isLocalLocked(2, false);
        assert !cache.isLocalLocked(3, false);

        assert !cache.isLocalLocked(1, true);
        assert !cache.isLocalLocked(2, true);
        assert !cache.isLocalLocked(3, true);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testGetPutRemove() throws IgniteCheckedException {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        int key = (int)System.currentTimeMillis();

        assert cache.get(key) == null;
        assert cache.getAndPut(key, "1") == null;

        String val = cache.get(key);

        assert val != null;
        assert "1".equals(val);

        val = cache.getAndRemove(key);

        assert val != null;
        assert "1".equals(val);
        assert cache.get(key) == null;
    }

    /**
     *
     * @throws Exception In case of error.
     */
    public void testPutWithExpiration() throws Exception {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        CacheEventListener lsnr = new CacheEventListener(new CountDownLatch(1));

        ignite.events().localListen(lsnr, EVTS_CACHE);

        ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, 200L));

        try {
            int key = (int)System.currentTimeMillis();

            cache.withExpiryPolicy(expiry).put(key, "val");

            assert cache.get(key) != null;

            cache.withExpiryPolicy(expiry).put(key, "val");

            Thread.sleep(500);

            assert cache.get(key) == null;
        }
        finally {
            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }
    }

    /**
     * Event listener.
     */
    private class CacheEventListener implements IgnitePredicate<Event> {
        /** Wait latch. */
        private CountDownLatch latch;

        /** Event types. */
        private int[] types;

        /**
         * @param latch Wait latch.
         * @param types Event types.
         */
        CacheEventListener(CountDownLatch latch, int... types) {
            this.latch = latch;
            this.types = types;

            if (F.isEmpty(types))
                this.types = new int[] { EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED };
        }

        /**
         * @param latch New latch.
         */
        void latch(CountDownLatch latch) {
            this.latch = latch;
        }

        /**
         * Waits for latch.
         *
         * @throws InterruptedException If got interrupted.
         */
        void await() throws InterruptedException {
            latch.await();
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            info("Grid cache event: " + evt);

            if (U.containsIntArray(types, evt.type()))
                latch.countDown();

            return true;
        }
    }
}