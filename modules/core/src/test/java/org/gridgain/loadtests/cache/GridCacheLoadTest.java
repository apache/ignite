/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.cache;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Cache load test.
 */
public final class GridCacheLoadTest extends GridCacheAbstractLoadTest {
    /** Memory test. */
    private static final boolean MEMORY = false;

    /** Load test. */
    private static final boolean LOAD = true;

    /** */
    private static final int KEY_RANGE = 1000;

    /** */
    private GridCacheLoadTest() {
        // No-op
    }

    /** Write closure. */
    private final CIX1<GridCacheProjection<Integer, Integer>> writeClos =
        new CIX1<GridCacheProjection<Integer, Integer>>() {
        @Override public void applyx(GridCacheProjection<Integer, Integer> cache)
            throws GridException {
            for (int i = 0; i < operationsPerTx; i++) {
                int kv = RAND.nextInt(KEY_RANGE);

                assert cache.putx(kv, kv);

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes");
            }
        }
    };

    /** Read closure. */
    private final CIX1<GridCacheProjection<Integer, Integer>> readClos =
        new CIX1<GridCacheProjection<Integer, Integer>>() {
        @Override public void applyx(GridCacheProjection<Integer, Integer> cache)
            throws GridException {
            for (int i = 0; i < operationsPerTx; i++) {
                int k = RAND.nextInt(KEY_RANGE);

                Integer v = cache.get(k);

                if (v != null && !v.equals(k))
                    error("Invalid value [k=" + k + ", v=" + v + ']');

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads");
            }
        }
    };

    /**
     * @return New byte array.
     */
    private byte[] newArray() {
        byte[] bytes = new byte[valSize];

        // Populate one byte.
        bytes[RAND.nextInt(valSize)] = 1;

        return bytes;
    }

    /**
     *
     */
    @SuppressWarnings({"ErrorNotRethrown", "InfiniteLoopStatement"})
    private void memoryTest() {
        Ignite ignite = G.grid();

        final GridCache<Integer, byte[]> cache = ignite.cache(null);

        assert cache != null;

        final AtomicInteger cnt = new AtomicInteger();

        try {
            GridTestUtils.runMultiThreaded(new Callable() {
                @Override public Object call() throws Exception {
                    while (true) {
                        int idx;

                        cache.putx(idx = cnt.getAndIncrement(), newArray());

                        if (idx % 1000 == 0)
                            info("Stored '" + idx + "' objects in cache [cache-size=" + cache.keySet().size() + ']');
                    }
                }
            }, threads, "memory-test-worker");
        }
        catch (OutOfMemoryError ignore) {
            info("Populated '" + cnt.get() + "' 1K objects into cache [cache-size=" + cache.keySet().size() + ']');
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args Command line.
     * @throws Exception If fails.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty(IgniteSystemProperties.GG_UPDATE_NOTIFIER, "false");

        System.out.println("Starting master node [params=" + Arrays.toString(args) + ']');

        String cfg = args.length >= 1 ? args[0] : CONFIG_FILE;
        String log = args.length >= 2 ? args[1] : LOG_FILE;

        final GridCacheLoadTest test = new GridCacheLoadTest();

        try (Ignite g = Ignition.start(test.configuration(cfg, log))) {
            System.gc();

            if (LOAD)
                test.loadTest(test.writeClos, test.readClos);

            G.grid().cache(null).clearAll();

            System.gc();

            if (MEMORY)
                test.memoryTest();
        }
    }
}
