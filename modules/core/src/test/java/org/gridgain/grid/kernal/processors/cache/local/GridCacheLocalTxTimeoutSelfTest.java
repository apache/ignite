/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 *
 */
public class GridCacheLocalTxTimeoutSelfTest extends GridCommonAbstractTest {
    /** Grid. */
    private Ignite ignite;

    /**
     * Start grid by default.
     */
    public GridCacheLocalTxTimeoutSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration c = super.getConfiguration();

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);
        c.getTransactionsConfiguration().setDefaultTxTimeout(50);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);

        c.setCacheConfiguration(cc);

        c.setNetworkTimeout(1000);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticReadCommitted() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticSerializable() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticReadCommitted() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticSerializable() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws GridException If test failed.
     */
    private void checkTransactionTimeout(GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation) throws Exception {

        boolean wasEx = false;

        GridCacheTx tx = null;

        try {
            GridCache<Integer, String> cache = ignite.cache(null);

            tx = cache.txStart(concurrency, isolation, 50, 0);

            cache.put(1, "1");

            Thread.sleep(100);

            cache.put(1, "2");

            tx.commit();
        }
        catch (GridCacheTxOptimisticException e) {
            info("Received expected optimistic exception: " + e.getMessage());

            wasEx = true;

            tx.rollback();
        }
        catch (GridCacheTxTimeoutException e) {
            info("Received expected timeout exception: " + e.getMessage());

            wasEx = true;

            tx.rollback();
        }

        assert wasEx;
    }
}
