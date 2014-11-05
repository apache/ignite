/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * This class provides basic tests for {@link GridCacheWriteBehindStore}.
 */
public class GridCacheWriteBehindStoreSelfTest extends GridCacheWriteBehindStoreAbstractSelfTest {
    /**
     * Tests correct store shutdown when underlying store fails,
     *
     * @throws Exception If failed.
     */
    public void testShutdownWithFailure() throws Exception {
        final AtomicReference<Exception> err = new AtomicReference<>();

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    delegate.setShouldFail(true);

                    initStore(2);

                    try {
                        store.put(null, 1, "val1");
                        store.put(null, 2, "val2");
                    }
                    finally {
                        shutdownStore();

                        delegate.setShouldFail(false);
                    }
                }
                catch (Exception e) {
                    err.set(e);
                }
            }
        }, 1).get();

        if (err.get() != null)
            throw err.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleStore() throws Exception {
        initStore(2);

        try {
            GridCacheTx tx = null;

            store.put(tx, 1, "v1");
            store.put(tx, 2, "v2");

            store.txEnd(tx, true);

            assertEquals("v1", store.load(null, 1));
            assertEquals("v2", store.load(null, 2));
            assertNull(store.load(null, 3));

            store.remove(tx, 1);

            store.txEnd(tx, true);

            assertNull(store.load(null, 1));
            assertEquals("v2", store.load(null, 2));
            assertNull(store.load(null, 3));
        }
        finally {
            shutdownStore();
        }
    }

    /**
     * Check that all values written to the store will be in underlying store after timeout or due to size limits.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullableProblems"})
    public void testValuePropagation() throws Exception {
        // Need to test size-based write.
        initStore(1);

        try {
            for (int i = 0; i < CACHE_SIZE * 2; i++)
                store.put(null, i, "val" + i);

            U.sleep(200);

            for (int i = 0; i < CACHE_SIZE; i++) {
                String val = delegate.load(null, i);

                assertNotNull("Value for [key= " + i + "] was not written in store", val);
                assertEquals("Invalid value [key=" + i + "]", "val" + i, val);
            }

            U.sleep(FLUSH_FREQUENCY + 300);

            for (int i = CACHE_SIZE; i < CACHE_SIZE * 2; i++) {
                String val = delegate.load(null, i);

                assertNotNull("Value for [key= " + i + "] was not written in store", val);
                assertEquals("Invalid value [key=" + i + "]", "val" + i, val);
            }
        }
        finally {
            shutdownStore();
        }
    }

    /**
     * Tests store behaviour under continuous put of the same key with different values.
     *
     * @throws Exception If failed
     */
    public void testContinuousPut() throws Exception {
        initStore(2);

        try {
            final AtomicBoolean running = new AtomicBoolean(true);

            final AtomicInteger actualPutCnt = new AtomicInteger();

            GridFuture<?> fut = multithreadedAsync(new Runnable() {
                @SuppressWarnings({"NullableProblems"})
                @Override public void run() {
                    try {
                        while (running.get()) {
                            for (int i = 0; i < CACHE_SIZE; i++) {
                                store.put(null, i, "val-0");

                                actualPutCnt.incrementAndGet();

                                store.put(null, i, "val" + i);

                                actualPutCnt.incrementAndGet();
                            }
                        }
                    }
                    catch (GridException e) {
                        error("Unexpected exception in put thread", e);

                        assert false;
                    }
                }
            }, 1, "put");

            U.sleep(FLUSH_FREQUENCY * 2 + 500);

            int delegatePutCnt = delegate.getPutAllCount();

            running.set(false);

            fut.get();

            log().info(">>> [putCnt = " + actualPutCnt.get() + ", delegatePutCnt=" + delegatePutCnt + "]");

            assertTrue("No puts were made to the underlying store", delegatePutCnt > 0);
            assertTrue("Too many puts were made to the underlying store", delegatePutCnt < actualPutCnt.get() / 10);
        }
        finally {
            shutdownStore();
        }

        // These checks must be done after the store shut down
        assertEquals("Invalid store size", CACHE_SIZE, delegate.getMap().size());

        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Invalid value stored", "val" + i, delegate.getMap().get(i));
    }

    /**
     * Tests that all values were put into the store will be written to the underlying store
     * after shutdown is called.
     *
     * @throws Exception If failed.
     */
    public void testShutdown() throws Exception {
        initStore(2);

        try {
            final AtomicBoolean running = new AtomicBoolean(true);

            GridFuture<?> fut = multithreadedAsync(new Runnable() {
                @SuppressWarnings({"NullableProblems"})
                @Override public void run() {
                    try {
                        while (running.get()) {
                            for (int i = 0; i < CACHE_SIZE; i++) {
                                store.put(null, i, "val-0");

                                store.put(null, i, "val" + i);
                            }
                        }
                    }
                    catch (GridException e) {
                        error("Unexpected exception in put thread", e);

                        assert false;
                    }
                }
            }, 1, "put");

            U.sleep(300);

            running.set(false);

            fut.get();
        }
        finally {
            shutdownStore();
        }

        // These checks must be done after the store shut down
        assertEquals("Invalid store size", CACHE_SIZE, delegate.getMap().size());

        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Invalid value stored", "val" + i, delegate.getMap().get(i));
    }

    /**
     * Tests that all values will be written to the underlying store
     * right in the same order as they were put into the store.
     *
     * @throws Exception If failed.
     */
    public void testBatchApply() throws Exception {
        delegate = new GridCacheTestStore(new ConcurrentLinkedHashMap<Integer, String>());

        initStore(1);

        List<Integer> intList = new ArrayList<>(CACHE_SIZE);

        try {
            for (int i = 0; i < CACHE_SIZE; i++) {
                store.put(null, i, "val" + i);

                intList.add(i);
            }
        }
        finally {
            shutdownStore();
        }

        Map<Integer, String> underlyingMap = delegate.getMap();

        assertTrue("Store map key set: " + underlyingMap.keySet(), F.eqOrdered(underlyingMap.keySet(), intList));
    }
}
