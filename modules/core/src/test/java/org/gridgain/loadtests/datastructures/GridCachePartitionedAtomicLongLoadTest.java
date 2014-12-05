/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.PESSIMISTIC;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Load test for atomic long.
 */
public class GridCachePartitionedAtomicLongLoadTest extends GridCommonAbstractTest {
    /** Test duration. */
    private static final long DURATION = 8 * 60 * 60 * 1000;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final AtomicInteger idx = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionsConfiguration().setDefaultTxIsolation(REPEATABLE_READ);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setStartSize(200);
        cc.setPreloadMode(GridCachePreloadMode.SYNC);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setEvictionPolicy(new GridCacheLruEvictionPolicy<>(1000));
        cc.setBackups(1);
        cc.setAffinity(new GridCacheConsistentHashAffinityFunction(true));
        cc.setAtomicSequenceReserveSize(10);
        cc.setEvictSynchronized(true);
        cc.setEvictNearSynchronized(true);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoad() throws Exception {
        startGrid();

        try {
            multithreaded(new AtomicCallable(), 50);
        }
        finally {
            stopGrid();
        }
    }

    /**
     *
     */
    private class AtomicCallable implements Callable<Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            Ignite ignite = grid();

            GridCache cache = ignite.cache(null);

            assert cache != null;

            GridCacheAtomicSequence seq = cache.dataStructures().atomicSequence("SEQUENCE", 0, true);

            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < DURATION && !Thread.currentThread().isInterrupted()) {
                GridCacheTx tx = cache.txStart();

                long seqVal = seq.incrementAndGet();

                int curIdx = idx.incrementAndGet();

                if (curIdx % 1000 == 0)
                    info("Sequence value [seq=" + seqVal + ", idx=" + curIdx + ']');

                tx.commit();
            }

            return true;
        }
    }
}
