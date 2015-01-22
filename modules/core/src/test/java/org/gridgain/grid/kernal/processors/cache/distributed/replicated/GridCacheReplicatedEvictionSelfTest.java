/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.apache.ignite.cache.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;

import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;
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
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

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

        for (int g = 0; g < gridCount(); g++) {
            for (int i = 0; i < KEYS; i++) {
                if (cache(g).entry(String.valueOf(i)).primary())
                    assertTrue(cache(g).evict(String.valueOf(i)));
            }
        }

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
