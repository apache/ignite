/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests for partitioned cache automatic eviction.
 */
public class GridCachePartitionedEvictionSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final boolean TEST_INFO = true;

    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final int EVICT_CACHE_SIZE = 1;

    /** */
    private static final int KEY_CNT = 100;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setEvictionPolicy(new GridCacheFifoEvictionPolicy(EVICT_CACHE_SIZE));
        cc.setNearEvictionPolicy(new GridCacheFifoEvictionPolicy(EVICT_CACHE_SIZE));
        cc.setSwapEnabled(false);

        // We set 1 backup explicitly.
        cc.setBackups(1);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @param node Node.
     * @return Cache.
     */
    private GridCacheProjection<String, Integer> cache(ClusterNode node) {
        return G.grid(node.id()).cache(null);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxPessimisticReadCommitted() throws Exception {
        doTestEviction(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxPessimisticRepeatableRead() throws Exception {
        doTestEviction(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxPessimisticSerializable() throws Exception {
        doTestEviction(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxOptimisticReadCommitted() throws Exception {
        doTestEviction(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxOptimisticRepeatableRead() throws Exception {
        doTestEviction(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxOptimisticSerializable() throws Exception {
        doTestEviction(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     * @param concurrency Tx concurrency.
     * @param isolation Tx isolation.
     */
    private void doTestEviction(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation)
        throws Exception {
        assert concurrency != null;
        assert isolation != null;

        // This condition should be "true", otherwise the test doesn't make sense.
        assert KEY_CNT >= EVICT_CACHE_SIZE;

        GridDhtCacheAdapter<String, Integer> dht0 = dht(cache(0));
        GridDhtCacheAdapter<String, Integer> dht1 = dht(cache(1));

        GridCacheAffinity<String> aff = dht0.affinity();

        for (int kv = 0; kv < KEY_CNT; kv++) {
            String key = String.valueOf(kv);

            GridCacheProjection<String, Integer> c = cache(aff.mapKeyToNode(key));

            try (GridCacheTx tx = c.txStart(concurrency, isolation)) {
                assert c.get(key) == null;

                c.put(key, kv);

                c.entry(key).timeToLive(10);

                assertEquals(Integer.valueOf(kv), c.get(key));

                tx.commit();
            }
        }

        if (TEST_INFO) {
            info("Printing keys in dht0...");

            for (String key : dht0.keySet())
                info("[key=" + key + ", primary=" +
                    F.eqNodes(grid(0).localNode(), aff.mapKeyToNode(key)) + ']');

            info("Printing keys in dht1...");

            for (String key : dht1.keySet())
                info("[key=" + key + ", primary=" +
                    F.eqNodes(grid(1).localNode(), aff.mapKeyToNode(key)) + ']');
        }

        assertEquals(EVICT_CACHE_SIZE, dht0.size());
        assertEquals(EVICT_CACHE_SIZE, dht1.size());

        assertEquals(0, near(cache(0)).nearSize());
        assertEquals(0, near(cache(1)).nearSize());
    }
}
