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

package org.apache.ignite.internal.processors.cache.local;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestThread;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Multithreaded local cache locking test.
 */
public class GridCacheLocalMultithreadedSelfTest extends GridCommonAbstractTest {
    /** Cache. */
    private IgniteCache<Integer, String> cache;

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

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(LOCAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If test fails.
     */
    public void testBasicLocks() throws Throwable {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                Lock lock = cache.lock(1);

                assert lock.tryLock(1000L, TimeUnit.MILLISECONDS);

                info("Locked key from thread: " + thread());

                Thread.sleep(50);

                info("Unlocking key from thread: " + thread());

                lock.unlock();

                info("Unlocked key from thread: " + thread());

                return null;
            }
        }, 10, "basic-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiLocks() throws Throwable {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                Set<Integer> keys = Sets.newHashSet(1, 2, 3);

                Lock lock = cache.lockAll(keys);

                lock.lock();

                info("Locked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                Thread.sleep(50);

                info("Unlocking key from thread: " + thread());

                lock.unlock();

                info("Unlocked keys from thread: " + thread());

                return null;
            }
        }, 10, "multi-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testSlidingKeysLocks() throws Throwable {
        final AtomicInteger cnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                int idx = cnt.incrementAndGet();

                Set<Integer> keys = Sets.newHashSet(idx, idx + 1, idx + 2, idx + 3);

                Lock lock = cache.lockAll(keys);

                lock.lock();

                info("Locked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                Thread.sleep(50);

                info("Unlocking key from thread [keys=" + keys + ", thread=" + thread() + ']');

                lock.unlock();

                info("Unlocked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                return null;
            }
        }, 10, "multi-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testSingleLockTimeout() throws Exception {
        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        final Lock lock = cache.lock(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                assert !cache.isLocalLocked(1, false);

                lock.lock();

                assert cache.isLocalLocked(1, true);
                assert cache.isLocalLocked(1, false);

                l1.countDown();

                l2.await();

                lock.unlock();

                assert !cache.isLocalLocked(1, true);
                assert !cache.isLocalLocked(1, false);

                return null;
            }
        }, "lock-timeout-1");

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                l1.await();

                assert cache.isLocalLocked(1, false);
                assert !cache.isLocalLocked(1, true);

                assert !lock.tryLock(100L, TimeUnit.MILLISECONDS);

                assert cache.isLocalLocked(1, false);
                assert !cache.isLocalLocked(1, true);

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

        assert !cache.isLocalLocked(1, false);
        assert !cache.isLocalLocked(1, true);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiLockTimeout() throws Exception {
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

                assert !cache.isLocalLocked(1, false);

                Collections.addAll(keys1, idx, idx + 1, idx + 2, idx + 3);

                Lock lock = cache.lockAll(keys1);

                lock.lock();

                for (Integer key : keys1) {
                    assert cache.isLocalLocked(key, false) : "Failed to acquire lock for key: " + key;
                    assert cache.isLocalLocked(key, true) : "Failed to acquire lock for key: " + key;
                }

                l1.countDown();

                l2.await();

                lock.unlock();

                for (Integer key : keys1) {
                    assert !cache.isLocalLocked(key, false);
                    assert !cache.isLocalLocked(key, true);
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
                    assert cache.isLocalLocked(key, false);
                    assert !cache.isLocalLocked(key, true);
                }

                // Lock won't be acquired due to timeout.
                assert !cache.lockAll(keys2).tryLock(100, TimeUnit.MILLISECONDS);

                for (Integer key : keys2) {
                    boolean locked = cache.isLocalLocked(key, false);

                    assert locked == keys1.contains(key) : "Lock failure for key [key=" + key +
                        ", locked=" + locked + ", keys1=" + keys1 + ']';

                    assert !cache.isLocalLocked(key, true);
                }

                l2.countDown();

                l3.await();

                for (Integer key : keys2) {
                    assert !cache.isLocalLocked(key, false);
                    assert !cache.isLocalLocked(key, true);
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
            assert !cache.isLocalLocked(key, false);
            assert !cache.isLocalLocked(key, true);
        }

        for (Integer key : keys2) {
            assert !cache.isLocalLocked(key, false);
            assert !cache.isLocalLocked(key, true);
        }
    }

    /**
     * @return Formatted string for current thread.
     */
    private String thread() {
        return "Thread [id=" + Thread.currentThread().getId() + ", name=" + Thread.currentThread().getName() + ']';
    }
}