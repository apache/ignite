/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Tests synchronous eviction for replicated cache.
 */
public class GridCacheReplicatedEvictionSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setEvictSynchronized(true);
        ccfg.setEvictSynchronizedKeyBufferSize(1);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictSynchronized() throws Exception {
        final int KEYS = 10;

        for (int i = 0; i < KEYS; i++)
            cache(0).put(String.valueOf(i), i);

        for (int g = 0 ; g < gridCount(); g++) {
            for (int i = 0; i < KEYS; i++)
                assertNotNull(cache(g).peek(String.valueOf(i)));
        }

        Collection<IgniteFuture<IgniteEvent>> futs = new ArrayList<>();

        for (int g = 0 ; g < gridCount(); g++)
            futs.add(waitForLocalEvent(grid(g).events(), nodeEvent(grid(g).localNode().id()), EVT_CACHE_ENTRY_EVICTED));

        for (int i = 0; i < KEYS; i++)
            assertTrue(cache(0).evict(String.valueOf(i)));

        for (IgniteFuture<IgniteEvent> fut : futs)
            fut.get(3000);

        boolean evicted = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (int g = 0 ; g < gridCount(); g++) {
                    for (int i = 0; i < KEYS; i++) {
                        if (cache(g).peek(String.valueOf(i)) != null) {
                            log.info("Non-null value, will wait [grid=" + g + ", key=" + i + ']');

                            return false;
                        }
                    }
                }

                return true;
            }
        }, 3000);

        assertTrue(evicted);
    }

    /**
     * @param nodeId Node id.
     * @return Predicate for events belonging to specified node.
     */
    private IgnitePredicate<IgniteEvent> nodeEvent(final UUID nodeId) {
        assert nodeId != null;

        return new P1<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent e) {
                info("Predicate called [e.nodeId()=" + e.node().id() + ", nodeId=" + nodeId + ']');

                return e.node().id().equals(nodeId);
            }
        };
    }
}
