/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.testframework.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Multithreaded update test with off heap enabled.
 */
public abstract class GridCacheOffHeapMultiThreadedUpdateAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    protected static volatile boolean failed;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setOffHeapMaxMemory(1024 * 1024);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setPortableEnabled(portableEnabled());

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        failed = false;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testTransform() throws Exception { // TODO GG-9141
        testTransform(keyForNode(0));

        if (gridCount() > 1)
            testTransform(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testTransform(final Integer key) throws Exception {
        final GridCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = 10_000;

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                    if (i % 500 == 0)
                        log.info("Iteration " + i);

                    cache.transform(key, new IncClosure());
                }

                return null;
            }
        }, THREADS, "transform");

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertEquals("Unexpected value for grid " + i, (Integer)(ITERATIONS_PER_THREAD * THREADS), val);
        }

        assertFalse(failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        testPut(keyForNode(0));

        if (gridCount() > 1)
            testPut(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testPut(final Integer key) throws Exception {
        final GridCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = iterations();

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                    if (i % 500 == 0)
                        log.info("Iteration " + i);

                    Integer val = cache.put(key, i);

                    assertNotNull(val);
                }

                return null;
            }
        }, THREADS, "put");

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertNotNull("Unexpected value for grid " + i, val);
        }

        assertFalse(failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxIfAbsent() throws Exception {
        testPutxIfAbsent(keyForNode(0));

        if (gridCount() > 1)
            testPutxIfAbsent(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testPutxIfAbsent(final Integer key) throws Exception {
        final GridCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = iterations();

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                    if (i % 500 == 0)
                        log.info("Iteration " + i);

                    assertFalse(cache.putxIfAbsent(key, 100));
                }

                return null;
            }
        }, THREADS, "putxIfAbsent");

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertEquals("Unexpected value for grid " + i, (Integer)0, val);
        }

        assertFalse(failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutWithFilter() throws Exception {
        testPutWithFilter(keyForNode(0));

        if (gridCount() > 1)
            testPutWithFilter(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testPutWithFilter(final Integer key) throws Exception {
        final GridCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = iterations();

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                    if (i % 500 == 0)
                        log.info("Iteration " + i);

                    assertTrue(cache.putx(key, i, new TestFilter()));
                }

                return null;
            }
        }, THREADS, "putWithFilter");

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertNotNull("Unexpected value for grid " + i, val);
        }

        assertFalse(failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGet() throws Exception {
        testPutGet(keyForNode(0));

        if (gridCount() > 1)
            testPutGet(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testPutGet(final Integer key) throws Exception {
        final GridCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = iterations();

        GridFuture<Long> putFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                    if (i % 1000 == 0)
                        log.info("Put iteration " + i);

                    assertTrue(cache.putx(key, i));
                }

                return null;
            }
        }, THREADS, "put");

        final AtomicBoolean stop = new AtomicBoolean();

        GridFuture<Long> getFut;

        try {
            getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int cnt = 0;

                    while (!stop.get()) {
                        if (++cnt % 5000 == 0)
                            log.info("Get iteration " + cnt);

                        assertNotNull(cache.get(key));
                    }

                    return null;
                }
            }, THREADS, "get");

            putFut.get();
        }
        finally {
            stop.set(true);
        }

        getFut.get();

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertNotNull("Unexpected value for grid " + i, val);
        }
    }

    /**
     * @param idx Node index.
     * @return Primary key for node.
     */
    protected Integer keyForNode(int idx) {
        Integer key0 = null;

        GridCache<Integer, Integer> cache = grid(0).cache(null);

        for (int i = 0; i < 10_000; i++) {
            if (cache.affinity().isPrimary(grid(idx).localNode(), i)) {
                key0 = i;

                break;
            }
        }

        assertNotNull(key0);

        return key0;
    }

    /**
     * @return Number of iterations.
     */
    protected int iterations() {
        return 10_000;
    }

    /**
     */
    protected static class IncClosure implements IgniteClosure<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer apply(Integer val) {
            if (val == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null in transform: " + val);

                return null;
            }

            return val + 1;
        }
    }

    /**
     */
    protected static class TestFilter implements IgnitePredicate<GridCacheEntry<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public boolean apply(GridCacheEntry<Integer, Integer> e) {
            if (e == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null entry in filter: " + e);

                return false;
            }
            else if (e.peek() == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null value in filter: " + e);

                return false;
            }

            return true;
        }
    }
}
