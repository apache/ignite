package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.db.RebalanceBlockingSPI;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
public class SysCacheInconsistency2Test extends GridCommonAbstractTest {
    /** Slow rebalance cache name. */
    private static final String SLOW_REBALANCE_CACHE = UTILITY_CACHE_NAME;

    /** Supply message latch. */
    private static final AtomicReference<CountDownLatch> SUPPLY_MESSAGE_LATCH = new AtomicReference<>();

    private static final AtomicReference<CountDownLatch> SUPPLY_SEND_LATCH = new AtomicReference<>();

    /** Listening logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** Stop. */
    private final AtomicBoolean stop = new AtomicBoolean(false);

    /**
     *
     */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setGridLogger(listeningLog);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setCommunicationSpi(new RebalanceBlockingSPI(SUPPLY_MESSAGE_LATCH, SLOW_REBALANCE_CACHE, SUPPLY_SEND_LATCH));

        return cfg;
    }

    @Test
    public void restartLeadToOverflowOfDefferetDeletedQueue() throws Exception {
        LogListener logLsnr = matches(String.format("Partition states validation has failed for group: %s", UTILITY_CACHE_NAME)).build();
        listeningLog.registerListener(logLsnr);

        IgniteEx node1 = startGrid(0);
        IgniteInternalCache<Object, Object> utilityCache = node1.context().cache().utilityCache();

        for(int i = 0; i < 1000; i++)
            utilityCache.putAsync(new KeyUtility("key-" + i), "Obj").get();


        startGrid(1);

        awaitPartitionMapExchange(true, true, null);

        stopGrid(1);

        utilityCache.clear();

        CountDownLatch stopRebalanceLatch = new CountDownLatch(1);

        CountDownLatch readyToSendLatch = new CountDownLatch(1);

        SUPPLY_SEND_LATCH.set(readyToSendLatch);

        SUPPLY_MESSAGE_LATCH.set(stopRebalanceLatch);

        startGrid(1);

        readyToSendLatch.await();

        utilityCache.put(new KeyUtility("key-" + 0), "Obj");

        stopRebalanceLatch.countDown();

        stopGrid(1);

        startGrid(1);


        assertFalse(waitForCondition(logLsnr::check, SECONDS.toMillis(30)));
    }


    /** */
    private static class KeyUtility extends GridCacheUtilityKey<KeyUtility> {
        /** */
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
