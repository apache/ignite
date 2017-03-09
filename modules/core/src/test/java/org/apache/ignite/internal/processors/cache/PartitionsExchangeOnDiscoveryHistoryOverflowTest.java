package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Test discovery history overflow.
 */
public class PartitionsExchangeOnDiscoveryHistoryOverflowTest extends IgniteCacheAbstractTest {
    /** */
    private static final int CACHES_COUNT = 50;
    /** */
    private static final int DISCOVERY_HISTORY_SIZE = 10;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty("IGNITE_DISCOVERY_HISTORY_SIZE", "" + DISCOVERY_HISTORY_SIZE);
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        Map<IgnitePredicate<? extends Event>, int[]> map = new HashMap<>();

        // To make partitions exchanges longer.
        map.put(new P1<Event>() {
            @Override public boolean apply(Event event) {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    // No op.
                }
                return false;
            }
        }, new int[]{EventType.EVT_NODE_JOINED, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT, DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT});

        cfg.setLocalEventListeners(map);

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testDynamicCacheCreation() throws Exception {
        IgniteInternalFuture[] futs = new IgniteInternalFuture[CACHES_COUNT];

        for (int i = 0; i < CACHES_COUNT; i ++) {
            final int cacheIdx = i;
            final int gridIdx = cacheIdx % gridCount();
            futs[i] = GridTestUtils.runAsync(new Callable<IgniteCache>() {
                @Override public IgniteCache call() throws Exception {
                    return grid(gridIdx).createCache(cacheConfiguration(gridIdx, cacheIdx));
                }
            });
        }

        for (IgniteInternalFuture fut : futs) {
            assertNotNull(fut);
            fut.get();
        }
    }

    /**
     * @param gridIdx Grid index.
     * @param cacheIdx Cache index.
     * @return Newly created cache configuration.
     * @throws Exception In case of error.
     */
    @NotNull private CacheConfiguration cacheConfiguration(int gridIdx, int cacheIdx) throws Exception {
        CacheConfiguration cfg = cacheConfiguration(getTestGridName(gridIdx));
        cfg.setName("dynamic-cache-" + cacheIdx);
        return cfg;
    }
}
