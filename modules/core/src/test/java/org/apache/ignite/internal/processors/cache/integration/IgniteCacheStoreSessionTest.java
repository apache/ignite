package org.apache.ignite.internal.processors.cache.integration;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.util.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public class IgniteCacheStoreSessionTest extends IgniteCacheAbstractTest {
    /** */
    private static volatile List<ExpectedData> expData;

    /** */
    private static final String CACHE_NAME1 = "cache1";

    private TestStore store = new TestStore();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected CacheStore<?, ?> cacheStore() {
        return store;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        assert cfg.getCacheConfiguration().length == 1;

        CacheConfiguration ccfg0 = cfg.getCacheConfiguration()[0];

        CacheConfiguration ccfg1 = cacheConfiguration(gridName);

        ccfg1.setName(CACHE_NAME1);

        cfg.setCacheConfiguration(ccfg0, ccfg1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        expData = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        expData = Collections.synchronizedList(new ArrayList<ExpectedData>());

        super.beforeTestsStarted();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreSession() throws Exception {
        testTxPut(jcache(0), null, null);

        testTxPut(ignite(0).jcache(CACHE_NAME1), null, null);

        testTxRemove(null, null);

        testTxPutRemove(null, null);

        for (IgniteTxConcurrency concurrency : F.asList(PESSIMISTIC)) {
            for (IgniteTxIsolation isolation : F.asList(REPEATABLE_READ)) {
                testTxPut(jcache(0), concurrency, isolation);

                testTxRemove(concurrency, isolation);

                testTxPutRemove(concurrency, isolation);
            }
        }
    }

    /**
     * @param cnt Keys count.
     * @return Keys.
     * @throws Exception If failed.
     */
    private List<Integer> testKeys(IgniteCache cache, int cnt) throws Exception {
        return primaryKeys(cache, cnt, 0);
    }

    /**
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @throws Exception If failed.
     */
    private void testTxPutRemove(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        log.info("Test tx put/remove [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        IgniteCache<Integer, Integer> cache = jcache(0);

        List<Integer> keys = testKeys(cache, 3);

        Integer key1 = keys.get(0);
        Integer key2 = keys.get(1);
        Integer key3 = keys.get(2);

        try (IgniteTx tx = startTx(concurrency, isolation)) {
            log.info("Do tx put1.");

            cache.put(key1, key1);

            log.info("Do tx put2.");

            cache.put(key2, key2);

            log.info("Do tx remove.");

            cache.remove(key3);

            expData.add(new ExpectedData("writeAll", new HashMap<>(), null));
            expData.add(new ExpectedData("delete", F.<Object, Object>asMap(0, "writeAll"), null));
            expData.add(new ExpectedData("txEnd", F.<Object, Object>asMap(0, "writeAll", 1, "delete"), null));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());
    }

    /**
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @throws Exception If failed.
     */
    private void testTxPut(IgniteCache<Object, Object> cache,
        IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation) throws Exception {
        log.info("Test tx put [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        List<Integer> keys = testKeys(cache, 3);

        Integer key1 = keys.get(0);

        try (IgniteTx tx = startTx(concurrency, isolation)) {
            log.info("Do tx get.");

            cache.get(key1);

            log.info("Do tx put.");

            cache.put(key1, key1);

            expData.add(new ExpectedData("write", new HashMap<>(), cache.getName()));
            expData.add(new ExpectedData("txEnd", F.<Object, Object>asMap(0, "write"), cache.getName()));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());

        Integer key2 = keys.get(1);
        Integer key3 = keys.get(2);

        try (IgniteTx tx = startTx(concurrency, isolation);) {
            log.info("Do tx put1.");

            cache.put(key2, key2);

            log.info("Do tx put2.");

            cache.put(key3, key3);

            expData.add(new ExpectedData("writeAll", new HashMap<>(), cache.getName()));
            expData.add(new ExpectedData("txEnd", F.<Object, Object>asMap(0, "writeAll"), cache.getName()));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());
    }

    /**
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @throws Exception If failed.
     */
    private void testTxRemove(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        log.info("Test tx remove [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        IgniteCache<Integer, Integer> cache = jcache(0);

        List<Integer> keys = testKeys(cache, 3);

        Integer key1 = keys.get(0);

        try (IgniteTx tx = startTx(concurrency, isolation)) {
            log.info("Do tx get.");

            cache.get(key1);

            log.info("Do tx remove.");

            cache.remove(key1, key1);

            expData.add(new ExpectedData("delete", new HashMap<>(), null));
            expData.add(new ExpectedData("txEnd", F.<Object, Object>asMap(0, "delete"), null));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());

        Integer key2 = keys.get(1);
        Integer key3 = keys.get(2);

        try (IgniteTx tx = startTx(concurrency, isolation);) {
            log.info("Do tx remove1.");

            cache.remove(key2, key2);

            log.info("Do tx remove2.");

            cache.remove(key3, key3);

            expData.add(new ExpectedData("deleteAll", new HashMap<>(), null));
            expData.add(new ExpectedData("txEnd", F.<Object, Object>asMap(0, "deleteAll"), null));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());
    }

    /**
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @return Transaction.
     */
    private IgniteTx startTx(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) {
        IgniteTransactions txs = ignite(0).transactions();

        if (concurrency == null)
            return txs.txStart();

        return txs.txStart(concurrency, isolation);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSessionCrossCacheTx() throws Exception {
        IgniteCache<Object, Object> cache0 = ignite(0).jcache(null);

        IgniteCache<Object, Object> cache1 = ignite(0).jcache(CACHE_NAME1);

        Integer key1 = primaryKey(cache0);
        Integer key2 = primaryKeys(cache1, 1, key1 + 1).get(0);

        try (IgniteTx tx = startTx(null, null)) {
            cache1.put(key2, key2);

            cache0.put(key1, key1);

            expData.add(new ExpectedData("writeAll", new HashMap<>(), null));
            expData.add(new ExpectedData("txEnd", F.<Object, Object>asMap(0, "writeAll"), null));

            tx.commit();
        }

        assertEquals(0, expData.size());
    }

    /**
     *
     */
    static class ExpectedData {
        /** */
        private final String expMtd;

        /** */
        private  final Map<Object, Object> expProps;

        /** */
        private final String expCacheName;

        /**
         * @param expMtd Expected method.
         * @param expProps Expected properties.
         * @param expCacheName Expected cache name.
         */
        public ExpectedData(String expMtd, Map<Object, Object> expProps, String expCacheName) {
            this.expMtd = expMtd;
            this.expProps = expProps;
            this.expCacheName = expCacheName;
        }
    }

    /**
     *
     */
    private class TestStore extends CacheStore<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, @Nullable Object... args) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public void txEnd(boolean commit) throws CacheWriterException {
            log.info("Tx end [commit=" + commit + ", tx=" + session().transaction() + ']');

            checkSession("txEnd");
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            log.info("Load [key=" + key + ", tx=" + session().transaction() + ']');

            checkSession("load");

            return key;
        }

        /** {@inheritDoc} */
        @Override public Map<Object, Object> loadAll(Iterable<?> keys) throws CacheLoaderException {
            log.info("LoadAll [keys=" + keys + ", tx=" + session().transaction() + ']');

            checkSession("loadAll");

            Map<Object, Object> loaded = new HashMap<>();

            for (Object key : keys)
                loaded.put(key, key);

            return loaded;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            log.info("Write [write=" + entry + ", tx=" + session().transaction() + ']');

            checkSession("write");
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) throws CacheWriterException {
            log.info("WriteAll: [writeAll=" + entries + ", tx=" + session().transaction() + ']');

            checkSession("writeAll");
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            log.info("Delete [key=" + key + ", tx=" + session().transaction() + ']');

            checkSession("delete");
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            log.info("DeleteAll [keys=" + keys + ", tx=" + session().transaction() + ']');

            checkSession("deleteAll");
        }

        /**
         * @param mtd Called stored method.
         */
        private void checkSession(String mtd) {
            assertFalse(expData.isEmpty());

            ExpectedData exp = expData.remove(0);

            assertEquals(exp.expMtd, mtd);

            CacheStoreSession ses = session();

            assertNotNull(ses);

            assertNotNull(ses.transaction());

            Map<Object, Object> props = ses.properties();

            assertNotNull(props);

            assertEquals(exp.expProps, props);

            props.put(props.size(), mtd);

            assertEquals(exp.expCacheName, ses.cacheName());
        }
    }
}
