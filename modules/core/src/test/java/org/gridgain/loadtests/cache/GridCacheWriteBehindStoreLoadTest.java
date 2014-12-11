/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Basic store test.
 */
public class GridCacheWriteBehindStoreLoadTest extends GridCommonAbstractTest {
    /** Flush frequency. */
    private static final int WRITE_FROM_BEHIND_FLUSH_FREQUENCY = 1000;

    /** Run time is 24 hours. */
    private static final long runTime = 24L * 60 * 60 * 60 * 1000;

    /** Specify if test keys should be randomly generated. */
    private boolean rndKeys;

    /** Number of distinct keys if they are generated randomly. */
    private int keysCnt = 20 * 1024;

    /** Number of threads that concurrently update cache. */
    private int threadCnt;

    /** No-op cache store. */
    private static final GridCacheStore store = new GridCacheStoreAdapter() {
        /** {@inheritDoc} */
        @Override public Object load(@Nullable GridCacheTx tx, Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable GridCacheTx tx, Object key,
            @Nullable Object val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable GridCacheTx tx, Object key) {
            // No-op.
        }
    };

    /**
     * Constructor
     */
    public GridCacheWriteBehindStoreLoadTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridCache<?, ?> cache = cache();

        if (cache != null)
            cache.clearAll();
    }

    /**
     * @return Caching mode.
     */
    protected GridCacheMode cacheMode() {
        return GridCacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected final IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration c = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);

        cc.setStore(store);

        cc.setWriteBehindEnabled(true);
        cc.setWriteBehindFlushFrequency(WRITE_FROM_BEHIND_FLUSH_FREQUENCY);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheSequentialKeys() throws Exception {
        rndKeys = false;

        threadCnt = 10;

        loadCache();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheRandomKeys() throws Exception {
        rndKeys = true;

        threadCnt = 10;

        loadCache();
    }

    /**
     * @throws Exception If failed.
     */
    private void loadCache() throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);

        final GridCache<Long, String> cache = cache();

        final AtomicLong keyCntr = new AtomicLong();

        long start = System.currentTimeMillis();

        IgniteFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings({"NullableProblems"})
            @Override public void run() {

                Random rnd = new Random();

                try {
                    while (running.get()) {
                        long putNum = keyCntr.incrementAndGet();

                        long key = rndKeys ? rnd.nextInt(keysCnt) : putNum;

                        cache.put(key, "val" + key);
                    }
                }
                catch (IgniteCheckedException e) {
                    error("Unexpected exception in put thread", e);

                    assert false;
                }
            }
        }, threadCnt, "put");

        long prevPutCnt = 0;

        while (System.currentTimeMillis() - start < runTime) {
            // Print stats every minute.
            U.sleep(60 * 1000);

            long cnt = keyCntr.get();
            long secondsElapsed = (System.currentTimeMillis() - start) / 1000;

            info(">>> Running for " + secondsElapsed + " seconds");
            info(">>> Puts: [total=" + cnt + ", avg=" + (cnt / secondsElapsed) + " (ops/sec), lastMinute=" +
                ((cnt - prevPutCnt) / 60) + "(ops/sec)]");

            prevPutCnt = cnt;
        }

        running.set(false);

        fut.get();
    }

    /**
     * @return Will return 0 to disable timeout.
     */
    @Override protected long getTestTimeout() {
        return 0;
    }
}
