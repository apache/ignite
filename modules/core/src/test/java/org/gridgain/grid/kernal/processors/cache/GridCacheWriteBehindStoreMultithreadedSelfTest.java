/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Multithreaded tests for {@link GridCacheWriteBehindStore}.
 */
public class GridCacheWriteBehindStoreMultithreadedSelfTest extends GridCacheWriteBehindStoreAbstractSelfTest {
    /**
     * This test performs complex set of operations on store from multiple threads.
     *
     * @throws Exception If failed.
     */
    public void testPutGetRemove() throws Exception {
        initStore(2);

        Set<Integer> exp;

        try {
            exp = runPutGetRemoveMultithreaded(10, 10);
        }
        finally {
            shutdownStore();
        }

        Map<Integer, String> map = delegate.getMap();

        Collection<Integer> extra = new HashSet<>(map.keySet());

        extra.removeAll(exp);

        assertTrue("The underlying store contains extra keys: " + extra, extra.isEmpty());

        Collection<Integer> missing = new HashSet<>(exp);

        missing.removeAll(map.keySet());

        assertTrue("Missing keys in the underlying store: " + missing, missing.isEmpty());

        for (Integer key : exp)
            assertEquals("Invalid value for key " + key, "val" + key, map.get(key));
    }

    /**
     * Tests that cache would keep values if underlying store fails.
     *
     * @throws Exception If failed.
     */
    public void testStoreFailure() throws Exception {
        delegate.setShouldFail(true);

        initStore(2);

        Set<Integer> exp;

        try {
            exp = runPutGetRemoveMultithreaded(10, 10);

            U.sleep(FLUSH_FREQUENCY);

            info(">>> There are " + store.getWriteBehindErrorRetryCount() + " entries in RETRY state");

            delegate.setShouldFail(false);

            // Despite that we set shouldFail flag to false, flush thread may just have caught an exception.
            // If we move store to the stopping state right away, this value will be lost. That's why this sleep
            // is inserted here to let all exception handlers in write-behind store exit.
            U.sleep(1000);
        }
        finally {
            shutdownStore();
        }

        Map<Integer, String> map = delegate.getMap();

        Collection<Integer> extra = new HashSet<>(map.keySet());

        extra.removeAll(exp);

        assertTrue("The underlying store contains extra keys: " + extra, extra.isEmpty());

        Collection<Integer> missing = new HashSet<>(exp);

        missing.removeAll(map.keySet());

        assertTrue("Missing keys in the underlying store: " + missing, missing.isEmpty());

        for (Integer key : exp)
            assertEquals("Invalid value for key " + key, "val" + key, map.get(key));
    }

    /**
     * Tests store consistency in case of high put rate, when flush is performed from the same thread
     * as put or remove operation.
     *
     * @throws Exception If failed.
     */
    public void testFlushFromTheSameThread() throws Exception {
        // 50 milliseconds should be enough.
        delegate.setOperationDelay(50);

        initStore(2);

        Set<Integer> exp;

        int start = store.getWriteBehindTotalCriticalOverflowCount();

        try {
            //We will have in total 5 * CACHE_SIZE keys that should be enough to grow map size to critical value.
            exp = runPutGetRemoveMultithreaded(5, CACHE_SIZE);
        }
        finally {
            log.info(">>> Done inserting, shutting down the store");

            shutdownStore();
        }

        // Restore delay.
        delegate.setOperationDelay(0);

        Map<Integer, String> map = delegate.getMap();

        int end = store.getWriteBehindTotalCriticalOverflowCount();

        log.info(">>> There are " + exp.size() + " keys in store, " + (end - start) + " overflows detected");

        assertTrue("No cache overflows detected (a bug or too few keys or too few delay?)", end > start);

        Collection<Integer> extra = new HashSet<>(map.keySet());

        extra.removeAll(exp);

        assertTrue("The underlying store contains extra keys: " + extra, extra.isEmpty());

        Collection<Integer> missing = new HashSet<>(exp);

        missing.removeAll(map.keySet());

        assertTrue("Missing keys in the underlying store: " + missing, missing.isEmpty());

        for (Integer key : exp)
            assertEquals("Invalid value for key " + key, "val" + key, map.get(key));
    }
}
