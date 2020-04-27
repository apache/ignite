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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Test discovery history overflow.
 */
public class PartitionsExchangeOnDiscoveryHistoryOverflowTest extends IgniteCacheAbstractTest {
    /** */
    private static final int CACHES_COUNT = 30;

    /** */
    private static final int DISCOVERY_HISTORY_SIZE = 10;

    /** */
    private static String histSize;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        histSize = System.getProperty(IGNITE_DISCOVERY_HISTORY_SIZE);

        System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, String.valueOf(DISCOVERY_HISTORY_SIZE));

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (histSize != null)
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, histSize);
        else
            System.getProperties().remove(IGNITE_DISCOVERY_HISTORY_SIZE);

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
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
            @Override public boolean apply(Event evt) {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    // No op.
                }

                return false;
            }
        }, new int[]{EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_DISCOVERY_CUSTOM_EVT});

        cfg.setLocalEventListeners(map);

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testDynamicCacheCreation() throws Exception {
        for (int iter = 0; iter < 5; iter++) {
            log.info("Iteration: " + iter);

            IgniteInternalFuture[] futs = new IgniteInternalFuture[CACHES_COUNT];

            for (int i = 0; i < CACHES_COUNT; i++) {
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

            for (int i = 0; i < CACHES_COUNT; i++) {
                final int cacheIdx = i;

                final int gridIdx = cacheIdx % gridCount();

                futs[i] = GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        grid(gridIdx).destroyCache(cacheConfiguration(gridIdx, cacheIdx).getName());

                        return null;
                    }
                });
            }

            for (IgniteInternalFuture fut : futs) {
                assertNotNull(fut);

                fut.get();
            }
        }
    }

    /**
     * @param gridIdx Grid index.
     * @param cacheIdx Cache index.
     * @return Newly created cache configuration.
     * @throws Exception In case of error.
     */
    @NotNull private CacheConfiguration cacheConfiguration(int gridIdx, int cacheIdx) throws Exception {
        CacheConfiguration cfg = cacheConfiguration(getTestIgniteInstanceName(gridIdx));

        cfg.setName("dynamic-cache-" + cacheIdx);

        return cfg;
    }
}
