/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.dsi.cacheget;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.util.concurrent.atomic.*;

/**
 * This is an adapted test case from DSI-49 (http://www.gridgainsystems.com/jira/browse/DSI-49).
 */
public class GridBenchmarkCacheGetLoadTest {
    /** */
    private static AtomicLong cnt = new AtomicLong();

    /** */
    private static AtomicLong latency = new AtomicLong();

    /** */
    private static AtomicLong id = new AtomicLong();

    private static Thread t;

    /**
     *
     */
    private GridBenchmarkCacheGetLoadTest() {
        // No-op.
    }

    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignition.start("modules/core/src/test/config/load/dsi-49-server-production.xml");

        GridCache<Long, Long> cache = Ignition.grid("dsi").cache("PARTITIONED_CACHE");

        stats();

        boolean usePrj = true;

        GridCacheProjection<Long, Long> cachePrj = cache.projection(Long.class, Long.class);

        for (int i = 0; i < 5000000; i++) {
            long t0 = System.currentTimeMillis();

            cnt.incrementAndGet();

            if (usePrj)
                // This is slow
                cachePrj.get(id.incrementAndGet());
            else
                // This is fast
                cache.get(id.incrementAndGet());

            latency.addAndGet(System.currentTimeMillis() - t0);
        }

        System.out.println("Finished test.");

        if (t != null) {
            t.interrupt();
            t.join();
        }
    }

    /**
     *
     */
    public static void stats() {
        t = new Thread(new Runnable() {
            @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
            @Override public void run() {
                int interval = 5;

                while (!Thread.currentThread().isInterrupted()) {
                    long cnt0 = cnt.get();
                    long lt0 = latency.get();

                    try {
                        Thread.sleep(interval * 1000);
                    }
                    catch (InterruptedException e) {
                        System.out.println("Stat thread got interrupted: " + e);

                        return;
                    }

                    long cnt1 = cnt.get();
                    long lt1 = latency.get();

                    System.out.println("Get/s: " + (cnt1 - cnt0) / interval);
                    System.out.println("Avg Latency: " + ((cnt1 - cnt0) > 0 ? (lt1 - lt0) / (cnt1 - cnt0) +
                        "ms" : "invalid"));
                }
            }
        });

        t.start();
    }
}
