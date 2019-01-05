package org.apache.ignite.internal.processors.cache.transactions;

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * <p> The <code>TxWithFilterTest</code> </p>
 *
 * @author Alexei Scherbakov
 */
@RunWith(JUnit4.class)
public class TxWithFilterTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 3;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        cfg.setFailureDetectionTimeout(100000000000L);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);
    }

    @Test
    public void test() {
        ignite(0).createCache(cacheConfiguration(PARTITIONED, 1, CacheAtomicityMode.TRANSACTIONAL));

        IgniteEx client = grid(NODES - 1);

        IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        assertNotNull(cache);

        int key = 0, val = 0;

        cache.put(key, val);

        try(Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE)) {
            Object prev = cache.getAndPutIfAbsent(key, val + 1);

            assertNotNull(prev);

            tx.commit();
        }

        assertEquals(Integer.valueOf(val), cache.get(key));

        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            PartitionUpdateCounter cntr = counter(client.affinity(DEFAULT_CACHE_NAME).partition(key), ignite.name());

            assertEquals("Bad counter for node=" + ignite.name(), 1, cntr.get());
        }
    }

    /**
     * @param partId Partition id.
     */
    protected PartitionUpdateCounter counter(int partId, String gridName) {
        return internalCache(grid(gridName).cache(DEFAULT_CACHE_NAME)).
            context().topology().localPartition(partId).dataStore().partUpdateCounter();
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        ccfg.setInterceptor(new TestInterceptor());

        return (CacheConfiguration)ccfg;
    }

    private static class TestInterceptor extends CacheInterceptorAdapter<Integer, Integer> {

    }
}
