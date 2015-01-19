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

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Multithreaded local cache locking test.
 */
public class GridCacheLocalMultithreadedSelfTest extends GridCommonAbstractTest {
    /** Cache. */
    private GridCache<Integer, String> cache;

    /**
     * Start grid by default.
     */
    public GridCacheLocalMultithreadedSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ignite = grid();

        cache = ignite.cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cache = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(LOCAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If test fails.
     */
    public void testBasicLocks() throws Throwable {
        final IgniteCache<Object, Object> cache = grid().jcache(null);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                assert cache.lock(1).tryLock(1000L, TimeUnit.MILLISECONDS);

                info("Locked key from thread: " + thread());

                Thread.sleep(50);

                info("Unlocking key from thread: " + thread());

                cache.lock(1).unlock();

                info("Unlocked key from thread: " + thread());

                return null;
            }
        }, 10, "basic-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiLocks() throws Throwable {
        final IgniteCache<Integer, String> cache = grid().jcache(null);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                Set<Integer> keys = new HashSet<Integer>();

                Collections.addAll(keys, 1, 2, 3);

                cache.lockAll(keys).lock();

                info("Locked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                Thread.sleep(50);

                info("Unlocking key from thread: " + thread());

                cache.lockAll(keys).unlock();

                info("Unlocked keys from thread: " + thread());

                return null;
            }
        }, 10, "multi-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testSlidingKeysLocks() throws Throwable {
        final IgniteCache<Integer, String> cache = grid().jcache(null);

        final AtomicInteger cnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                int idx = cnt.incrementAndGet();

                Set<Integer> keys = new HashSet<>();

                Collections.addAll(keys, idx, idx + 1, idx + 2, idx + 3);

                cache.lockAll(keys).lock();

                info("Locked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                Thread.sleep(50);

                info("Unlocking key from thread [keys=" + keys + ", thread=" + thread() + ']');

                cache.lockAll(keys).unlock();

                info("Unlocked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                return null;
            }
        }, 10, "multi-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testSingleLockTimeout() throws Exception {
        final IgniteCache<Object, Object> cache = grid().jcache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                assert !cache.isLocked(1);

                cache.lock(1).lock();

                assert cache.isLockedByThread(1);
                assert cache.isLocked(1);

                l1.countDown();

                l2.await();

                cache.lock(1).unlock();

                assert !cache.isLockedByThread(1);
                assert !cache.isLocked(1);

                return null;
            }
        }, "lock-timeout-1");

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                l1.await();

                assert cache.isLocked(1);
                assert !cache.isLockedByThread(1);

                assert !cache.lock(1).tryLock(100L, TimeUnit.MILLISECONDS);

                assert cache.isLocked(1);
                assert !cache.isLockedByThread(1);

                l2.countDown();

                info("Checked lockedByThread.");

                return null;
            }
        }, "lock-timeout-2");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        assert !cache.isLocked(1);
        assert !cache.isLockedByThread(1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiLockTimeout() throws Exception {
        final IgniteCache<Integer, String> cache = grid().jcache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        final CountDownLatch l3 = new CountDownLatch(1);

        final AtomicInteger cnt = new AtomicInteger();

        final Set<Integer> keys1 = new HashSet<>();
        final Set<Integer> keys2 = new HashSet<>();

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                int idx = cnt.incrementAndGet();

                assert !cache.isLocked(1);

                Collections.addAll(keys1, idx, idx + 1, idx + 2, idx + 3);

                cache.lockAll(keys1).lock();

                for (Integer key : keys1) {
                    assert cache.isLocked(key) : "Failed to acquire lock for key: " + key;
                    assert cache.isLockedByThread(key) : "Failed to acquire lock for key: " + key;
                }

                l1.countDown();

                l2.await();

                cache.lockAll(keys1).unlock();

                for (Integer key : keys1) {
                    assert !cache.isLocked(key);
                    assert !cache.isLockedByThread(key);
                }

                l3.countDown();

                return null;
            }
        }, "lock-timeout-1");

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                int idx = cnt.incrementAndGet();

                Collections.addAll(keys2, idx, idx + 1, idx + 2, idx + 3);

                l1.await();

                for (Integer key : keys1) {
                    assert cache.isLocked(key);
                    assert !cache.isLockedByThread(key);
                }

                // Lock won't be acquired due to timeout.
                assert !cache.lockAll(keys2).tryLock(100, TimeUnit.MILLISECONDS);

                for (Integer key : keys2) {
                    boolean locked = cache.isLocked(key);

                    assert locked == keys1.contains(key) : "Lock failure for key [key=" + key +
                        ", locked=" + locked + ", keys1=" + keys1 + ']';

                    assert !cache.isLockedByThread(key);
                }

                l2.countDown();

                l3.await();

                for (Integer key : keys2) {
                    assert !cache.isLocked(key);
                    assert !cache.isLockedByThread(key);
                }

                return null;
            }
        }, "lock-timeout-2");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        for (Integer key : keys1) {
            assert !cache.isLocked(key);
            assert !cache.isLockedByThread(key);
        }

        for (Integer key : keys2) {
            assert !cache.isLocked(key);
            assert !cache.isLockedByThread(key);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockOrder() throws Exception {
        final IgniteCache<Object, Object> cache = grid().jcache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        final CountDownLatch l3 = new CountDownLatch(1);

        Thread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                cache.lock(1).lock();

                l1.countDown();

                assert cache.isLocked(1);
                assert cache.isLockedByThread(1);

                l2.await();

                cache.lock(1).unlock();

                l3.countDown();

                assert !cache.isLockedByThread(1);

                return null;
            }
        });

        Thread t2 = new Thread(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    l1.await();

                    CacheLock lock1 = cache.enableAsync().lock(1);

                    IgniteFuture<Boolean> f1 = cache.enableAsync().future();

                    try {
                        f1.get(100, TimeUnit.MILLISECONDS);

                        assert false;
                    }
                    catch (IgniteFutureTimeoutException e) {
                        info("Correctly received timeout exception: " + e);
                    }

                    CacheLock lock2 = cache.lock(2);

                    lock2.lock();

                    IgniteFuture<Boolean> f2 = lock2.future();

                    try {
                        // Can't acquire f2 because f1 is held.
                        f2.get(100, TimeUnit.MILLISECONDS);

                        // TODO uncomment after GG-3756 fix
                        //assert false;
                    }
                    catch (IgniteFutureTimeoutException e) {
                        info("Correctly received timeout exception: " + e);
                    }

                    assert lock1.isLocked();
                    assert !lock1.isLockedByThread();

                    // TODO uncomment after GG-3756 fix
                    //assert cache.isLocked(2);
                    //assert !cache.isLockedByThread(2);

                    l2.countDown();

                    l3.await();

                    assert f1.get();

                    assert cache.isLocked(1);
                    assert cache.isLockedByThread(1);

                    assert f2.get();

                    assert cache.isLocked(2);
                    assert cache.isLockedByThread(2);

                    lock1.unlock();
                    lock2.unlock();

                    assert !cache.isLocked(1);
                    assert !cache.isLockedByThread(1);

                    assert !cache.isLocked(2);
                    assert !cache.isLockedByThread(2);
                }
                catch (Throwable e) {
                    error("Failed to acquire lock in thread: " + thread(), e);

                    fail("Failed to acquire lock in thread: " + thread());
                }
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assert !cache.isLocked(1);
        assert !cache.isLockedByThread(1);

        assert !cache.isLocked(2);
        assert !cache.isLockedByThread(2);
    }

    /**
     * @return Formatted string for current thread.
     */
    private String thread() {
        return "Thread [id=" + Thread.currentThread().getId() + ", name=" + Thread.currentThread().getName() + ']';
    }
}
