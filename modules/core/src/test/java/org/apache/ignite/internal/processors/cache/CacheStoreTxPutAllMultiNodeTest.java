package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class CacheStoreTxPutAllMultiNodeTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private Ignite client;

    /** */
    private IgniteCache<Integer, String> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("client"))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        startGrid(1);
        startGrid(2);

        client = startGrid("client");

        cache = client.getOrCreateCache(cacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPutAllInTransaction() throws Exception {
        try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {

            tx.timeout(10_000);

            cache.putAll(createMap());

            tx.commit();
        }
        catch (Exception ex) {
            System.out.println("Expected exception: " + ex.getMessage() + ", suppressed=" + Arrays.toString(ex.getSuppressed()));
        }

        startGrid(3);

        IgniteTxManager txMgr = ((IgniteEx) client).context().cache().context().tm();

        long curTime = U.currentTimeMillis();

        for (IgniteInternalTx tx : txMgr.activeTransactions())
            assertTrue(curTime - tx.startTime() < tx.timeout());

        assertTrue(client.transactions().localActiveTransactions().isEmpty());

    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, String> cacheConfiguration() {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>(CACHE_NAME);

        cfg.setCacheStoreFactory(new CacheStoreTxPutAllMultiNodeTest.StoreFactory());

        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        return cfg;
    }

    /**
     * @return Map to put.
     */
    private static Map<Integer, String> createMap() {
        Map<Integer, String> data = new TreeMap<>();

        for (int i = 1; i < 500; i ++)
            data.put(i, "Eddy " + i);

        return data;
    }

    /**
     *
     */
    private static class StoreFactory implements Factory<CacheStore<? super Integer, ? super String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public CacheStore<? super Integer, ? super String> create() {
            return new CacheStoreTxPutAllMultiNodeTest.TestStore();
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, String> implements Serializable {
        /** */
        private final ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<>();

        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-injected store session. */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /**
         *
         */
        public TestStore() {
            for (int i = -100; i < 1000; i++)
                map.put(i, String.valueOf(i));

            map.put(1000, "key");
        }

        /** {@inheritDoc} */
        @Override public String load(Integer key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends String> entry) throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SuspiciousMethodCalls")
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            Transaction tx = transaction();

            throw new CacheWriterException("SessionEnd cache store is closed." + tx);
        }

        /**
         * @return Current transaction.
         */
        private @Nullable Transaction transaction() {
            CacheStoreSession ses = session();

            return ses != null ? ses.transaction() : null;
        }

        /**
         * @return Store session.
         */
        protected CacheStoreSession session() {
            return ses;
        }
    }
}