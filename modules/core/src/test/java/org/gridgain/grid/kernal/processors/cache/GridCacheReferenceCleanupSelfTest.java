/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.testframework.GridTestUtils.*;

/**
 *
 */
public class GridCacheReferenceCleanupSelfTest extends GridCommonAbstractTest {
    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Cache mode for the current test. */
    private GridCacheMode mode;

    /** */
    private boolean cancel;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(mode);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setDgcRemoveLocks(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testAtomicLongPartitioned() throws Exception {
        mode = GridCacheMode.PARTITIONED;

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
        mode = GridCacheMode.REPLICATED;

        startGrids(2);

        try {
            checkReferenceCleanup(atomicLongCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void _testAtomicLongLocal() throws Exception { // TODO GG-9141
        mode = GridCacheMode.LOCAL;

        try {
            checkReferenceCleanup(atomicLongCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testOneAsyncOpPartitioned() throws Exception {
        mode = GridCacheMode.PARTITIONED;

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
        mode = GridCacheMode.REPLICATED;

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
        mode = GridCacheMode.LOCAL;

        try {
            checkReferenceCleanup(oneAsyncOpCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSeveralAsyncOpsPartitioned() throws Exception {
        mode = GridCacheMode.PARTITIONED;

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
        mode = GridCacheMode.REPLICATED;

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
        mode = GridCacheMode.LOCAL;

        try {
            checkReferenceCleanup(severalAsyncOpsCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testSyncOpAsyncCommitPartitioned() throws Exception {
        mode = GridCacheMode.PARTITIONED;

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
        mode = GridCacheMode.REPLICATED;

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
        mode = GridCacheMode.LOCAL;

        try {
            checkReferenceCleanup(syncOpAsyncCommitCallable());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testAsyncOpsAsyncCommitPartitioned() throws Exception {
        mode = GridCacheMode.PARTITIONED;

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
        mode = GridCacheMode.REPLICATED;

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
        mode = GridCacheMode.LOCAL;

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
                        assertNull(ref.get());
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
                    GridCache<Integer, TestValue> cache = g.cache(null);

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    Map<Integer, TestValue> m = new HashMap<>();

                    for (int i = 0; i < 10; i++) {
                        TestValue val = new TestValue(i);

                        refs.add(new WeakReference<Object>(val));

                        m.put(i, val);

                        cache.dataStructures().atomicLong("testLong" + i, 0, true).incrementAndGet();
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
                    GridCache<Integer, TestValue> cache = g.cache(null);

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    TestValue val = new TestValue(0);

                    refs.add(new WeakReference<Object>(val));

                    cache.putxAsync(0, val).get();
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
                    GridCache<Integer, TestValue> cache = g.cache(null);

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    Collection<IgniteFuture<?>> futs = new ArrayList<>(1000);

                    for (int i = 0; i < 1000; i++) {
                        TestValue val = new TestValue(i);

                        refs.add(new WeakReference<Object>(val));

                        futs.add(cache.putxAsync(i, val));
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
                    GridCache<Integer, TestValue> cache = g.cache(null);

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    GridCacheTx tx = cache.txStart();

                    TestValue val = new TestValue(0);

                    refs.add(new WeakReference<Object>(val));

                    cache.putx(0, val);

                    tx.commitAsync().get();
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
                    GridCache<Integer, TestValue> cache = g.cache(null);

                    refs.add(new WeakReference<Object>(cacheContext(cache)));

                    GridCacheTx tx = cache.txStart();

                    for (int i = 0; i < 1000; i++) {
                        TestValue val = new TestValue(i);

                        refs.add(new WeakReference<Object>(val));

                        cache.putxAsync(i, val);
                    }

                    tx.commitAsync().get();
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
