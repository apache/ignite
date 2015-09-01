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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestThread;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Test cases for multi-threaded tests.
 */
@SuppressWarnings({"ProhibitedExceptionThrown"})
public class GridCacheLocalLockSelfTest extends GridCommonAbstractTest {
    /** Grid. */
    private Ignite ignite;

    /**
     *
     */
    public GridCacheLocalLockSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;
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
     * @throws IgniteCheckedException If test failed.
     */
    public void testLockReentry() throws IgniteCheckedException {
        IgniteCache<Integer, String> cache = ignite.cache(null);

        assert !cache.isLocalLocked(1, false);
        assert !cache.isLocalLocked(1, true);

        Lock lock = cache.lock(1);

        lock.lock();

        assert cache.isLocalLocked(1, false);
        assert cache.isLocalLocked(1, true);

        try {
            assert cache.get(1) == null;
            assert cache.getAndPut(1, "1") == null;
            assert "1".equals(cache.get(1));

            // Reentry.
            lock.lock();

            assert cache.isLocalLocked(1, false);
            assert cache.isLocalLocked(1, true);

            try {
                assert "1".equals(cache.getAndRemove(1));
            }
            finally {
                lock.unlock();
            }

            assert cache.isLocalLocked(1, false);
            assert cache.isLocalLocked(1, true);
        }
        finally {
            lock.unlock();
        }

        assert !cache.isLocalLocked(1, false);
        assert !cache.isLocalLocked(1, true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLock() throws Throwable {
        final IgniteCache<Integer, String> cache = ignite.cache(null);

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);

        final Lock lock = cache.lock(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            @SuppressWarnings({"CatchGenericClass"})
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for.key 1");

                lock.lock();

                info("After lock for key 1");

                try {
                    assert cache.isLocalLocked(1, false);
                    assert cache.isLocalLocked(1, true);

                    latch1.countDown();

                    info("Let thread2 proceed.");

                    cache.put(1, "1");

                    info("Put 1='1' key pair into cache.");

                    latch2.await();

                    info("Waited for latch 2");
                }
                finally {
                    lock.unlock();

                    info("Unlocked entry for key 1.");

                    latch3.countDown();
                }

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            @SuppressWarnings({"CatchGenericClass"})
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                latch1.await();

                info("Latch1 released.");

                assert !lock.tryLock();

                assert cache.isLocalLocked(1, false);
                assert !cache.isLocalLocked(1, true);

                info("Tried to lock cache for key1");

                latch2.countDown();

                info("Released latch2");

                latch3.await();

                assert lock.tryLock();

                assert cache.isLocalLocked(1, false);
                assert cache.isLocalLocked(1, true);

                try {
                    info("Locked cache for key 1");

                    assert "1".equals(cache.get(1));

                    info("Read value for key 1");

                    assert "1".equals(cache.getAndRemove(1));

                    info("Removed value for key 1");

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

        assert !cache.isLocalLocked(1, true);
        assert !cache.isLocalLocked(1, false);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockAndPut() throws Throwable {
        final IgniteCache<Integer, String> cache = ignite.cache(null);

        final CountDownLatch latch1 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                Lock lock = cache.lock(1);

                lock.lock();

                info("Locked cache key: 1");

                try {
                    assert cache.isLocalLocked(1, true);
                    assert cache.isLocalLocked(1, false);

                    info("Verified that cache key is locked: 1");

                    cache.put(1, "1");

                    info("Put key value pair into cache: 1='1'");

                    latch1.countDown();

                    info("Released latch1");

                    // Hold lock for a bit.
                    Thread.sleep(50);

                    info("Woke up from sleep.");
                }
                finally {
                    lock.unlock();

                    info("Unlocked cache key: 1");
                }

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                info("Beginning to await on latch 1");

                latch1.await();

                info("Finished awaiting on latch 1");

                assert "1".equals(cache.get(1));

                info("Retrieved value from cache for key: 1");

                cache.put(1, "2");

                info("Put key-value pair into cache: 1='2'");

                assert "2".equals(cache.getAndRemove(1));

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

        assert !cache.isLocalLocked(1, true);
        assert !cache.isLocalLocked(1, false);
    }
}