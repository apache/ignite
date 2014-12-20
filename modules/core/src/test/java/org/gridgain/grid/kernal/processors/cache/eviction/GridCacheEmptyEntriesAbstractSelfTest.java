/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.GridCacheEvictionPolicy;
import org.gridgain.grid.cache.eviction.fifo.GridCacheFifoEvictionPolicy;
import org.gridgain.grid.cache.store.GridCacheStore;
import org.gridgain.grid.cache.store.GridCacheStoreAdapter;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.U;
import org.gridgain.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;

/**
 * Tests that cache handles {@code setAllowEmptyEntries} flag correctly.
 */
public abstract class GridCacheEmptyEntriesAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private GridCacheEvictionPolicy<?, ?> plc;

    /** */
    private GridCacheEvictionPolicy<?, ?> nearPlc;

    /** Test store. */
    private GridCacheStore<String, String> testStore;

    /** Tx concurrency to use. */
    private GridCacheTxConcurrency txConcurrency;

    /** Tx isolation to use. */
    private GridCacheTxIsolation txIsolation;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TransactionsConfiguration txCfg = c.getTransactionsConfiguration();

        txCfg.setDefaultTxConcurrency(txConcurrency);
        txCfg.setDefaultTxIsolation(txIsolation);
        txCfg.setTxSerializableEnabled(true);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);

        cc.setEvictionPolicy(plc);
        cc.setNearEvictionPolicy(nearPlc);
        cc.setEvictSynchronizedKeyBufferSize(1);

        cc.setEvictNearSynchronized(true);
        cc.setEvictSynchronized(true);

        cc.setStore(testStore);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * Starts grids depending on testing cache.
     *
     * @return First grid node.
     * @throws Exception If failed.
     */
    protected abstract Ignite startGrids() throws Exception;

    /** @return Cache mode for particular test. */
    protected abstract GridCacheMode cacheMode();

    /**
     * Tests FIFO eviction policy.
     *
     * @throws Exception If failed.
     */
    public void testFifo() throws Exception {
        plc = new GridCacheFifoEvictionPolicy(50);
        nearPlc = new GridCacheFifoEvictionPolicy(50);

        checkPolicy();
    }

    /**
     * Checks policy with and without store set.
     *
     * @throws Exception If failed.
     */
    private void checkPolicy() throws Exception {
        testStore = null;

        checkPolicy0();

        testStore = new GridCacheStoreAdapter<String, String>() {
            @Override public String load(@Nullable GridCacheTx tx, String key) {
                return null;
            }

            @Override public void put(@Nullable GridCacheTx tx, String key,
                @Nullable String val) {
                // No-op.
            }

            @Override public void remove(@Nullable GridCacheTx tx, String key) {
                // No-op.
            }
        };

        checkPolicy0();
    }

    /**
     * Tests preset eviction policy.
     *
     * @throws Exception If failed.
     */
    private void checkPolicy0() throws Exception {
        for (GridCacheTxConcurrency concurrency : GridCacheTxConcurrency.values()) {
            txConcurrency = concurrency;

            for (GridCacheTxIsolation isolation : GridCacheTxIsolation.values()) {
                txIsolation = isolation;

                Ignite g = startGrids();

                GridCache<String, String> cache = g.cache(null);

                try {
                    info(">>> Checking policy [txConcurrency=" + txConcurrency + ", txIsolation=" + txIsolation +
                        ", plc=" + plc + ", nearPlc=" + nearPlc + ']');

                    checkExplicitTx(cache);

                    checkImplicitTx(cache);
                }
                finally {
                    stopAllGrids();
                }
            }
        }
    }

    /**
     * Checks that gets work for implicit txs.
     *
     * @param cache Cache to test.
     * @throws Exception If failed.
     */
    private void checkImplicitTx(GridCache<String, String> cache) throws Exception {
        assertNull(cache.get("key1"));
        assertNull(cache.getAsync("key2").get());

        assertTrue(cache.getAll(F.asList("key3", "key4")).isEmpty());
        assertTrue(cache.getAllAsync(F.asList("key5", "key6")).get().isEmpty());

        cache.put("key7", "key7");
        cache.remove("key7", "key7");
        assertNull(cache.get("key7"));

        checkEmpty(cache);
    }

    /**
     * Checks that gets work for implicit txs.
     *
     * @param cache Cache to test.
     * @throws Exception If failed.
     */
    private void checkExplicitTx(GridCache<String, String> cache) throws Exception {
        GridCacheTx tx = cache.txStart();

        try {
            assertNull(cache.get("key1"));

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            assertNull(cache.getAsync("key2").get());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            assertTrue(cache.getAll(F.asList("key3", "key4")).isEmpty());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            assertTrue(cache.getAllAsync(F.asList("key5", "key6")).get().isEmpty());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            cache.put("key7", "key7");

            cache.remove("key7");

            assertNull(cache.get("key7"));

            tx.commit();
        }
        finally {
            tx.close();
        }

        checkEmpty(cache);
    }

    /**
     * Checks that cache is empty.
     *
     * @param cache Cache to check.
     * @throws GridInterruptedException If interrupted while sleeping.
     */
    @SuppressWarnings({"ErrorNotRethrown", "TypeMayBeWeakened"})
    private void checkEmpty(GridCache<String, String> cache) throws GridInterruptedException {
        for (int i = 0; i < 3; i++) {
            try {
                assertTrue(cache.entrySet().toString(), cache.entrySet().isEmpty());

                break;
            }
            catch (AssertionError e) {
                if (i == 2)
                    throw e;

                info(">>> Cache is not empty, flushing evictions.");

                U.sleep(1000);
            }
        }
    }
}
