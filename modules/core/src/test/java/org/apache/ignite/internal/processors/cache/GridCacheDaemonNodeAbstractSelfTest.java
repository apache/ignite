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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test cache operations with daemon node.
 */
public abstract class GridCacheDaemonNodeAbstractSelfTest extends GridCommonAbstractTest {
    /** Daemon flag. */
    protected boolean daemon;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        daemon = false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setDaemon(daemon);

        c.setConnectorConfiguration(null);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(new NearCacheConfiguration());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * Returns cache mode specific for test.
     *
     * @return Cache configuration.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testImplicit() throws Exception {
        try {
            startGridsMultiThreaded(3);

            daemon = true;

            startGrid(4);

            IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 30; i++)
                cache.put(i, i);

            Map<Integer, Integer> batch = new HashMap<>();

            for (int i = 30; i < 60; i++)
                batch.put(i, i);

            cache.putAll(batch);

            for (int i = 0; i < 60; i++)
                assertEquals(i, (int)cache.get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExplicit() throws Exception {
        try {
            startGridsMultiThreaded(3);

            daemon = true;

            startGrid(4);

            IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 30; i++) {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(i, i);

                    tx.commit();
                }
            }

            Map<Integer, Integer> batch = new HashMap<>();

            for (int i = 30; i < 60; i++)
                batch.put(i, i);

            try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.putAll(batch);
                tx.commit();
            }

            for (int i = 0; i < 60; i++)
                assertEquals(i, (int)cache.get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test mapKeyToNode() method for normal and daemon nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMapKeyToNode() throws Exception {
        try {
            // Start normal nodes.
            Ignite g1 = startGridsMultiThreaded(3);

            // Start daemon node.
            daemon = true;

            final Ignite g2 = startGrid(4);

            for (long i = 0; i < Integer.MAX_VALUE; i = (i << 1) + 1) {
                // Call mapKeyToNode for normal node.
                assertNotNull(g1.<Long>affinity(DEFAULT_CACHE_NAME).mapKeyToNode(i));

                // Call mapKeyToNode for daemon node.
                final long i0 = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return g2.<Long>affinity(DEFAULT_CACHE_NAME).mapKeyToNode(i0);
                    }
                }, IgniteException.class, "Failed to find cache");
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
