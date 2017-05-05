package org.apache.ignite.cache.database;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 *
 */
public class IgnitePersistentStoreTxCacheRebalancingTest extends IgnitePersistentStoreCacheRebalancingAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setBackups(1);
        ccfg.setRebalanceDelay(10_000);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        explicitTx = false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChangesWithConstantLoadExplicitTx() throws Exception {
        explicitTx = true;

        testTopologyChangesWithConstantLoad();
    }
}
