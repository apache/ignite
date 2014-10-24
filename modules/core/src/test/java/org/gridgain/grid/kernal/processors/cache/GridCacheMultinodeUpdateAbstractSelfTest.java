/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;

/**
 * Multinode update test.
 */
public abstract class GridCacheMultinodeUpdateAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    protected static volatile boolean failed;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected GridCacheStore<?, ?> cacheStore() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransform() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        final Integer key = primaryKey(cache);

        cache.put(key, 0);

        final int THREADS = gridCount();
        final int ITERATIONS_PER_THREAD = 1000;

        Integer expVal = 0;

        for (int i = 0; i < iterations(); i++) {
            log.info("Iteration: " + i);

            final AtomicInteger gridIdx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = gridIdx.incrementAndGet() - 1;

                    final GridCache<Integer, Integer> cache = grid(idx).cache(null);

                    for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++)
                        cache.transform(key, new IncClosure());

                    return null;
                }
            }, THREADS, "transform");

            assertFalse("Got null in transform.", failed);

            expVal += ITERATIONS_PER_THREAD * THREADS;

            for (int j = 0; j < gridCount(); j++) {
                Integer val = (Integer)grid(j).cache(null).get(key);

                assertEquals("Unexpected value for grid " + j, expVal, val);
            }
        }
    }

    /**
     * @return Number of iterations.
     */
    protected int iterations() {
        return atomicityMode() == ATOMIC ? 30 : 15;
    }

    /**
     *
     */
    protected static class IncClosure implements GridClosure<Integer, Integer> {
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
}
