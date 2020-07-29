package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.db.RebalanceBlockingSPI;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class SysCacheInconsistencyInternalKeyTest extends GridCommonAbstractTest {
    /** Slow rebalance cache name. */
    private static final String SLOW_REBALANCE_CACHE = UTILITY_CACHE_NAME;

    /** Supply message latch. */
    private static final AtomicReference<CountDownLatch> SUPPLY_MESSAGE_LATCH = new AtomicReference<>();

    /** Supply send latch. */
    private static final AtomicReference<CountDownLatch> SUPPLY_SEND_LATCH = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setCommunicationSpi(new RebalanceBlockingSPI(SUPPLY_MESSAGE_LATCH, SLOW_REBALANCE_CACHE, SUPPLY_SEND_LATCH));

        return cfg;
    }

    /**
     * Checks that {@link GridCacheInternal} must be added to delete queue.
     */
    @Test
    public void restartLeadToProblemWithDeletedQueue() throws Exception {
        IgniteEx node1 = startGrid(0);
        IgniteInternalCache<Object, Object> utilityCache = node1.context().cache().utilityCache();

        for (int i = 0; i < 1000; i++)
            utilityCache.putAsync(new KeyUtility("key-" + i), "Obj").get();

        CountDownLatch stopRebalanceLatch = new CountDownLatch(1);

        CountDownLatch readyToSndLatch = new CountDownLatch(1);

        SUPPLY_SEND_LATCH.set(readyToSndLatch);

        SUPPLY_MESSAGE_LATCH.set(stopRebalanceLatch);

        runAsync(() -> startGrid(1));

        readyToSndLatch.await();

        for (int i = 0; i < 1000; i++)
            utilityCache.remove(new KeyUtility("key-" + i));

        stopRebalanceLatch.countDown();

        awaitPartitionMapExchange(true, true, null);

        assertFalse(idleVerify(node1, UTILITY_CACHE_NAME).hasConflicts());
    }

    /**
     *
     */
    private static class KeyUtility extends GridCacheUtilityKey<KeyUtility> {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Inner key. */
        private String innerKey;

        /**
         * @param key Key.
         */
        private KeyUtility(String key) {
            innerKey = key;
        }

        /** {@inheritDoc} */
        @Override protected boolean equalsx(KeyUtility key) {
            return key.innerKey.equals(innerKey);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return innerKey.hashCode();
        }
    }
}
