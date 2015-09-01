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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.cacheContext;

/**
 *
 */
public class GridCacheReferenceCleanupSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache mode for the current test. */
    private CacheMode mode;

    /** */
    private boolean cancel;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(mode);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testAtomicLongPartitioned() throws Exception {
        mode = CacheMode.PARTITIONED;

        startGrids(2);

        try {
            checkReferenceCleanup(atomicLongCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testAtomicLongReplicated() throws Exception {
        mode = CacheMode.REPLICATED;

        startGrids(2);

        try {
            checkReferenceCleanup(atomicLongCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testAtomicLongLocal() throws Exception {
        mode = CacheMode.LOCAL;

        try {
            checkReferenceCleanup(atomicLongCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testOneAsyncOpPartitioned() throws Exception {
        mode = CacheMode.PARTITIONED;

        startGrids(2);

        try {
            checkReferenceCleanup(oneAsyncOpCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testOneAsyncOpReplicated() throws Exception {
        mode = CacheMode.REPLICATED;

        startGrids(2);

        try {
            checkReferenceCleanup(oneAsyncOpCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testOneAsyncOpLocal() throws Exception {
        mode = CacheMode.LOCAL;

        try {
            checkReferenceCleanup(oneAsyncOpCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSeveralAsyncOpsPartitioned() throws Exception {
        mode = CacheMode.PARTITIONED;

        startGrids(2);

        try {
            checkReferenceCleanup(severalAsyncOpsCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSeveralAsyncOpsReplicated() throws Exception {
        mode = CacheMode.REPLICATED;

        startGrids(2);

        try {
            checkReferenceCleanup(severalAsyncOpsCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSeveralAsyncOpsLocal() throws Exception {
        mode = CacheMode.LOCAL;

        try {
            checkReferenceCleanup(severalAsyncOpsCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSyncOpAsyncCommitPartitioned() throws Exception {
        mode = CacheMode.PARTITIONED;

        startGrids(2);

        try {
            checkReferenceCleanup(syncOpAsyncCommitCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSyncOpAsyncCommitReplicated() throws Exception {
        mode = CacheMode.REPLICATED;

        startGrids(2);

        try {
            checkReferenceCleanup(syncOpAsyncCommitCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSyncOpAsyncCommitLocal() throws Exception {
        mode = CacheMode.LOCAL;

        try {
            checkReferenceCleanup(syncOpAsyncCommitCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testAsyncOpsAsyncCommitPartitioned() throws Exception {
        mode = CacheMode.PARTITIONED;

        startGrids(2);

        try {
            checkReferenceCleanup(asyncOpsAsyncCommitCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testAsyncOpsAsyncCommitReplicated() throws Exception {
        mode = CacheMode.REPLICATED;

        startGrids(2);

        try {
            checkReferenceCleanup(asyncOpsAsyncCommitCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testAsyncOpsAsyncCommitLocal() throws Exception {
        mode = CacheMode.LOCAL;

        try {
            checkReferenceCleanup(asyncOpsAsyncCommitCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param call Callable.
     * @throws Exception If failed.
     */
    public void checkReferenceCleanup(Callable<Collection<WeakReference<Object>>> call) throws Exception {
        for (boolean cancel : new boolean[] {true, false}) {
            this.cancel = cancel;

            final Collection<WeakReference<Object>> refs = call.call();

            GridTestUtils.retryAssert(log, 10, 1000, new CA() {
                @Override public void apply() {
                    System.gc();
                    System.gc();
                    System.gc();

                    for (WeakReference<?> ref : refs)
                        assertNull("" + ref.get(), ref.get());
                }
            });
        }
    }

    /**
     * Crates callable for atomic long test.
     *
     * @return Callable.
     * @throws Exception If failed.
     */
    private Callable<Collection<WeakReference<Object>>> atomicLongCallable() throws Exception {
        return new Callable<Collection<WeakReference<Object>>>() {
            @Override public Collection<WeakReference<Object>> call() throws Exception {
                Collection<WeakReference<Object>> refs = new ArrayList<>();

                Ignite g = startGrid();

                try {
                    IgniteCache<Integer, TestValue> cache = g.cache(null);

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    Map<Integer, TestValue> m = new HashMap<>();

                    for (int i = 0; i < 10; i++) {
                        TestValue val = new TestValue(i);

                        refs.add(new WeakReference<Object>(val));

                        m.put(i, val);

                        g.atomicLong("testLong" + i, 0, true).incrementAndGet();
                    }

                    cache.putAll(m);
                }
                finally {
                    G.stop(g.name(), cancel);
                }

                return refs;
            }
        };
    }

    /**
     * Crates callable for one async op test.
     *
     * @return Callable.
     * @throws Exception If failed.
     */
    private Callable<Collection<WeakReference<Object>>> oneAsyncOpCallable() throws Exception {
        return new Callable<Collection<WeakReference<Object>>>() {
            @Override public Collection<WeakReference<Object>> call() throws Exception {
                Collection<WeakReference<Object>> refs = new ArrayList<>();

                Ignite g = startGrid();

                try {
                    IgniteCache<Integer, TestValue> cache = g.cache(null);
                    IgniteCache<Integer, TestValue> cacheAsync = cache.withAsync();

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    TestValue val = new TestValue(0);

                    refs.add(new WeakReference<Object>(val));

                    cacheAsync.putIfAbsent(0, val);

                    cacheAsync.future().get();
                }
                finally {
                    G.stop(g.name(), cancel);
                }

                return refs;
            }
        };
    }

    /**
     * Crates callable for several async ops test.
     *
     * @return Callable.
     * @throws Exception If failed.
     */
    private Callable<Collection<WeakReference<Object>>> severalAsyncOpsCallable() throws Exception {
        return new Callable<Collection<WeakReference<Object>>>() {
            @Override public Collection<WeakReference<Object>> call() throws Exception {
                Collection<WeakReference<Object>> refs = new ArrayList<>();

                Ignite g = startGrid();

                try {
                    IgniteCache<Integer, TestValue> cache = g.cache(null);
                    IgniteCache<Integer, TestValue> cacheAsync = cache.withAsync();

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    Collection<IgniteFuture<?>> futs = new ArrayList<>(1000);

                    for (int i = 0; i < 1000; i++) {
                        TestValue val = new TestValue(i);

                        refs.add(new WeakReference<Object>(val));

                        cacheAsync.putIfAbsent(0, val);

                        futs.add(cacheAsync.future());
                    }

                    for (IgniteFuture<?> fut : futs)
                        fut.get();
                }
                finally {
                    G.stop(g.name(), cancel);
                }

                return refs;
            }
        };
    }

    /**
     * Crates callable for sync op with async commit test.
     *
     * @return Callable.
     * @throws Exception If failed.
     */
    private Callable<Collection<WeakReference<Object>>> syncOpAsyncCommitCallable() throws Exception {
        return new Callable<Collection<WeakReference<Object>>>() {
            @Override public Collection<WeakReference<Object>> call() throws Exception {
                Collection<WeakReference<Object>> refs = new ArrayList<>();

                Ignite g = startGrid();

                try {
                    IgniteCache<Integer, TestValue> cache = g.cache(null);

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    Transaction tx = g.transactions().txStart();

                    TestValue val = new TestValue(0);

                    refs.add(new WeakReference<Object>(val));

                    cache.put(0, val);

                    tx.commit();
                }
                finally {
                    G.stop(g.name(), cancel);
                }

                return refs;
            }
        };
    }

    /**
     * Crates callable for async ops with async commit test.
     *
     * @return Callable.
     * @throws Exception If failed.
     */
    private Callable<Collection<WeakReference<Object>>> asyncOpsAsyncCommitCallable() throws Exception {
        return new Callable<Collection<WeakReference<Object>>>() {
            @Override public Collection<WeakReference<Object>> call() throws Exception {
                Collection<WeakReference<Object>> refs = new ArrayList<>();

                Ignite g = startGrid();

                try {
                    IgniteCache<Integer, TestValue> cache = g.cache(null);
                    IgniteCache<Integer, TestValue> cacheAsync = cache.withAsync();

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    Transaction tx = g.transactions().txStart();

                    for (int i = 0; i < 1000; i++) {
                        TestValue val = new TestValue(i);

                        refs.add(new WeakReference<Object>(val));

                        cacheAsync.put(i, val);

                        cacheAsync.future().get();
                    }

                    tx.commit();
                }
                finally {
                    G.stop(g.name(), cancel);
                }

                return refs;
            }
        };
    }

    /** Test value class. Created mostly to simplify heap dump analysis. */
    private static class TestValue {
        /** */
        @SuppressWarnings("UnusedDeclaration")
        private final int i;

        /** @param i Value. */
        private TestValue(int i) {
            this.i = i;
        }
    }
}