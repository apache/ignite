/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.log4j.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Test getting the same value twice within the same transaction.
 */
public class GridCacheNearMultiGetSelfTest extends GridCommonAbstractTest {
    /** Cache debug flag. */
    private static final boolean CACHE_DEBUG = false;

    /** Number of gets. */
    private static final int GET_CNT = 5;

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);

        cc.setPreloadMode(NONE);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);
        spi.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        c.setDiscoverySpi(spi);

        c.setCacheConfiguration(cc);

        if (CACHE_DEBUG)
            resetLog4j(Level.DEBUG, false, GridCacheProcessor.class.getPackage().getName());

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
        for (int i = 0; i < GRID_CNT; i++) {
            Ignite g = grid(i);

            GridCache<Integer, String> c = g.cache(null);

            c.removeAll();

            assertEquals("Cache size mismatch for grid [grid=" + g.name() + ", entrySet=" + c.entrySet() + ']',
                0, c.size());
        }
    }

    /** @return {@code True} if debug enabled. */
    private boolean isTestDebug() {
        return true;
    }

    /** @throws Exception If failed. */
    public void testOptimisticReadCommittedNoPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, false);
    }

    /** @throws Exception If failed. */
    public void testOptimisticReadCommittedWithPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticReadCommitted() throws Exception {
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, false);
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticRepeatableReadNoPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /** @throws Exception If failed. */
    public void testOptimisticRepeatableReadWithPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticRepeatableRead() throws Exception {
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, false);
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticSerializableNoPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, false);
    }

    /** @throws Exception If failed. */
    public void testOptimisticSerializableWithPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticSerializable() throws Exception {
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, false);
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticReadCommittedNoPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, false);
    }

    /** @throws Exception If failed. */
    public void testPessimisticReadCommittedWithPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticReadCommitted() throws Exception {
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, false);
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticRepeatableReadNoPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /** @throws Exception If failed. */
    public void testPessimisticRepeatableReadWithPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticRepeatableRead() throws Exception {
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, false);
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticSerializableNoPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, false);
    }

    /** @throws Exception If failed. */
    public void testPessimisticSerializableWithPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticSerializable() throws Exception {
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, false);
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param put If {@code true}, then value will be pre-stored in cache.
     * @throws Exception If failed.
     */
    private void checkDoubleGet(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation, boolean put)
        throws Exception {
        GridCache<Integer, String> cache = grid(0).cache(null);

        Integer key = 1;

        String val = null;

        if (put)
            cache.put(key, val = Integer.toString(key));

        GridCacheTx tx = cache.txStart(concurrency, isolation, 0, 0);

        try {
            if (isTestDebug()) {
                info("Started transaction.");

                GridCacheAffinity<Integer> aff = cache.affinity();

                int part = aff.partition(key);

                if (isTestDebug())
                    info("Key affinity [key=" + key + ", partition=" + part + ", affinity=" +
                        U.toShortString(aff.mapKeyToPrimaryAndBackups(key)) + ']');
            }

            for (int i = 0; i < GET_CNT; i++) {
                if (isTestDebug())
                    info("Reading key [key=" + key + ", i=" + i + ']');

                String v = cache.get(key);

                assertEquals("Value mismatch for put [val=" + val + ", v=" + v + ", put=" + put + ']', val, v);

                if (isTestDebug())
                    info("Read value for key (will read again) [key=" + key + ", val=" + v + ", i=" + i + ']');
            }

            if (isTestDebug())
                info("Committing transaction.");

            tx.commit();

            if (isTestDebug())
                info("Committed transaction: " + tx);
        }
        catch (GridCacheTxOptimisticException e) {
            if (concurrency != OPTIMISTIC || isolation != SERIALIZABLE) {
                error("Received invalid optimistic failure.", e);

                throw e;
            }

            if (isTestDebug())
                info("Optimistic transaction failure (will rollback) [msg=" + e.getMessage() +
                    ", tx=" + tx.xid() + ']');

            try {
                tx.rollback();
            }
            catch (GridException ex) {
                error("Failed to rollback optimistic failure: " + tx, ex);

                throw ex;
            }
        }
        catch (Exception e) {
            error("Transaction failed (will rollback): " + tx, e);

            tx.rollback();

            throw e;
        }
        catch (Error e) {
            error("Error when executing transaction (will rollback): " + tx, e);

            tx.rollback();

            throw e;
        }
        finally {
            GridCacheTx t = cache.tx();

            assert t == null : "Thread should not have transaction upon completion ['t==tx'=" + (t == tx) +
                ", t=" + t + (t != tx ? "tx=" + tx : "tx=''") + ']';
        }
    }
}
