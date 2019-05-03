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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for replicated cache with one node.
 */
public class GridCacheContinuousQueryReplicatedTxOneNodeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setAtomicityMode(atomicMode());
        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        // TODO IGNITE-9530 Remove this clause.
        if (atomicMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            cacheCfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @return Atomicity mode for a cache.
     */
    protected CacheAtomicityMode atomicMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * @return Cache mode.
     */
    protected CacheMode cacheMode() {
        return CacheMode.REPLICATED;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocal() throws Exception {
        if (cacheMode() == CacheMode.REPLICATED)
            doTest(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDistributed() throws Exception {
        doTest(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalOneNode() throws Exception {
        doTestOneNode(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedOneNode() throws Exception {
        doTestOneNode(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest(boolean loc) throws Exception {
        try {
            IgniteCache<String, Integer> cache = startGrid(0).cache(DEFAULT_CACHE_NAME);

            ContinuousQuery<String, Integer> qry = new ContinuousQuery<>();

            final AtomicInteger cnt = new AtomicInteger();
            final CountDownLatch latch = new CountDownLatch(10);

            qry.setLocalListener(new CacheEntryUpdatedListener<String, Integer>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends Integer>> evts)
                        throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends String, ? extends Integer> evt : evts) {
                        cnt.incrementAndGet();
                        latch.countDown();
                    }
                }
            });

            cache.query(qry.setLocal(loc));

            startGrid(1);

            awaitPartitionMapExchange();

            for (int i = 0; i < 10; i++)
                cache.put("key" + i, i);

            assert latch.await(5000, TimeUnit.MILLISECONDS);

            assertEquals(10, cnt.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestOneNode(boolean loc) throws Exception {
        try {
            IgniteCache<String, Integer> cache = startGrid(0).cache(DEFAULT_CACHE_NAME);

            ContinuousQuery<String, Integer> qry = new ContinuousQuery<>();

            final AtomicInteger cnt = new AtomicInteger();
            final CountDownLatch latch = new CountDownLatch(10);

            for (int i = 0; i < 10; i++)
                cache.put("key" + i, i);

            if (atomicMode() != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
                cache.clear();
            else { // TODO IGNITE-7952. Remove "else" clause - do cache.clear() instead of iteration.
                for (Iterator it = cache.iterator(); it.hasNext();) {
                    it.next();
                    it.remove();
                }
            }

            qry.setLocalListener(new CacheEntryUpdatedListener<String, Integer>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends Integer>> evts)
                        throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends String, ? extends Integer> evt : evts) {
                        cnt.incrementAndGet();
                        latch.countDown();
                    }
                }
            });

            cache.query(qry.setLocal(loc));

            for (int i = 0; i < 10; i++)
                cache.put("key" + i, i);

            assert latch.await(5000, TimeUnit.MILLISECONDS);

            assertEquals(10, cnt.get());
        }
        finally {
            stopAllGrids();
        }
    }
}
