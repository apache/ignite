package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;

public class HandlingUncheckedExceptionOnRebalanceTest extends GridCommonAbstractTest {
    /** Node failure occurs. */
    private final CountDownLatch failure = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setIncludeEventTypes(EVT_CACHE_REBALANCE_OBJECT_LOADED)
            .setCacheConfiguration(
                new CacheConfiguration()
                    .setName(DEFAULT_CACHE_NAME)
                    .setCacheMode(CacheMode.REPLICATED))
            .setFailureHandler((i, f) -> {
                failure.countDown();

                return true;
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceUncheckedError() throws Exception {
        startGrid(0).getOrCreateCache(DEFAULT_CACHE_NAME).put(1,1);

        CountDownLatch latch = new CountDownLatch(1);

        startGrid(1).events().localListen(e -> {
            latch.countDown();

            throw new Error();
        }, EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED);

        assertTrue("Exception was not thrown.", latch.await(3, TimeUnit.SECONDS));
        assertTrue("Rebalancing does not handle unchecked exceptions by failure handler",
            failure.await(3, TimeUnit.SECONDS));
    }
}
