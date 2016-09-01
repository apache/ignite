package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;

/**
 * Indexing Spi transactional query test
 */
public class IndexingSpiQueryTxSelfTest extends GridCacheAbstractSelfTest {

    private static AtomicInteger cnt;

    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cnt = new AtomicInteger();

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        if (cnt.getAndIncrement() == 0)
            cfg.setClientMode(true);
        else {
            cfg.setIndexingSpi(new MyBrokenIndexingSpi());

            CacheConfiguration ccfg = cacheConfiguration(gridName);
            ccfg.setName("test-cache");
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ccfg.setIndexedTypes(Integer.class, Integer.class);

            cfg.setCacheConfiguration(ccfg);
        }
        return cfg;
    }

    /** Reproducing bug from IGNITE-2881 */
    public void testIndexingSpiWithTx() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, Integer> cache = ignite.cache("test-cache");

        int i = 0;

        IgniteTransactions txs = ignite.transactions();

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                System.out.println("Run in transaction: " + concurrency + " " + isolation);

                final int val = ++i;

                GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Transaction tx = null;

                        try (Transaction tx0 = tx = txs.txStart(concurrency, isolation)) {
                            cache.put(val, val);

                            tx0.commit();
                        }

                        assertEquals(TransactionState.ROLLED_BACK, tx.state());
                        return null;
                    }
                }, IgniteTxHeuristicCheckedException.class);
            }
        }
    }

    /**
     * Indexing Spi implementation for test
     */
    private static class MyBrokenIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        private final SortedMap<Object, Object> index = new TreeMap<>();

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String spaceName, Collection<Object> params,
            @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
            throw new IgniteSpiException("Test exception");
        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String spaceName, Object key, Object val, long expirationTime)
            throws IgniteSpiException {
            index.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String spaceName, Object key) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onSwap(@Nullable String spaceName, Object key) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onUnswap(@Nullable String spaceName, Object key, Object val) throws IgniteSpiException {
            // No-op.
        }
    }
}
