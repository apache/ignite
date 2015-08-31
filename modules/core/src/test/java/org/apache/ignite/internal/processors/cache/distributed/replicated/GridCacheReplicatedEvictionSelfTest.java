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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;

/**
 * Tests synchronous eviction for replicated cache.
 */
public class GridCacheReplicatedEvictionSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
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
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
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
            jcache(0).put(String.valueOf(i), i);

        for (int g = 0 ; g < gridCount(); g++) {
            for (int i = 0; i < KEYS; i++)
                assertNotNull(jcache(g).localPeek(String.valueOf(i)));
        }

        Collection<IgniteFuture<Event>> futs = new ArrayList<>();

        for (int g = 0 ; g < gridCount(); g++)
            futs.add(waitForLocalEvent(grid(g).events(), nodeEvent(grid(g).localNode().id()), EVT_CACHE_ENTRY_EVICTED));

        for (int g = 0; g < gridCount(); g++) {
            for (int i = 0; i < KEYS; i++) {
                if (grid(g).affinity(null).isPrimary(grid(g).localNode(), String.valueOf(i)))
                    jcache(g).localEvict(Collections.singleton(String.valueOf(i)));
            }
        }

        for (IgniteFuture<Event> fut : futs)
            fut.get(3000);

        boolean evicted = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (int g = 0 ; g < gridCount(); g++) {
                    for (int i = 0; i < KEYS; i++) {
                        if (jcache(g).localPeek(String.valueOf(i)) != null) {
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
    private IgnitePredicate<Event> nodeEvent(final UUID nodeId) {
        assert nodeId != null;

        return new P1<Event>() {
            @Override public boolean apply(Event e) {
                info("Predicate called [e.nodeId()=" + e.node().id() + ", nodeId=" + nodeId + ']');

                return e.node().id().equals(nodeId);
            }
        };
    }
}