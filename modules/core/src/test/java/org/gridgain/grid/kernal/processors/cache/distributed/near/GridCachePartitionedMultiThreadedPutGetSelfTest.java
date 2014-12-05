/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Multithreaded partition cache put get test.
 */
public class GridCachePartitionedMultiThreadedPutGetSelfTest extends GridCommonAbstractTest {
    /** */
    private static final boolean TEST_INFO = true;

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Number of threads. */
    private static final int THREAD_CNT = 10;

    /** Number of transactions per thread. */
    private static final int TX_CNT = 500;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setEvictionPolicy(new GridCacheFifoEvictionPolicy<>(1000));
        cc.setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy());
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);
        cc.setEvictSynchronized(false);
        cc.setEvictNearSynchronized(false);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < GRID_CNT; i++) {
            grid(i).cache(null).removeAll();

            assert grid(i).cache(null).isEmpty();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommitted() throws Exception {
        doTest(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableRead() throws Exception {
        doTest(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticSerializable() throws Exception {
        doTest(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommitted() throws Exception {
        doTest(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableRead() throws Exception {
        doTest(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticSerializable() throws Exception {
        doTest(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope", "PointlessBooleanExpression"})
    private void doTest(final GridCacheTxConcurrency concurrency, final GridCacheTxIsolation isolation)
        throws Exception {
        final AtomicInteger cntr = new AtomicInteger();

        multithreaded(new CAX() {
            @SuppressWarnings({"BusyWait"})
            @Override public void applyx() throws GridException {
                GridCache<Integer, Integer> c = grid(0).cache(null);

                for (int i = 0; i < TX_CNT; i++) {
                    int kv = cntr.incrementAndGet();

                    try (GridCacheTx tx = c.txStart(concurrency, isolation)) {
                        assertNull(c.get(kv));

                        assert c.putx(kv, kv);

                        assertEquals(Integer.valueOf(kv), c.get(kv));

                        // Again.
                        assert c.putx(kv, kv);

                        assertEquals(Integer.valueOf(kv), c.get(kv));

                        tx.commit();
                    }

                    if (TEST_INFO && kv % 1000 == 0)
                        info("Transactions: " + kv);
                }
            }
        }, THREAD_CNT);
    }
}
