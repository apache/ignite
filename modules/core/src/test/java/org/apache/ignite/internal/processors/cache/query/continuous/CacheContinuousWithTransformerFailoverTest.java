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

import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.EventListener;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 */
public class CacheContinuousWithTransformerFailoverTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerNodeLeft() throws Exception {
        startGrids(3);

        final int CLIENT_ID = 3;

        Ignite clnNode = startClientGrid(CLIENT_ID);

        IgniteOutClosure<IgniteCache<Integer, Integer>> cache =
            new IgniteOutClosure<IgniteCache<Integer, Integer>>() {
                int cnt = 0;

                @Override public IgniteCache<Integer, Integer> apply() {
                    ++cnt;

                    return grid(CLIENT_ID).cache(DEFAULT_CACHE_NAME);
                }
            };

        final CacheEventListener lsnr = new CacheEventListener();

        ContinuousQueryWithTransformer<Object, Object, String> qry = new ContinuousQueryWithTransformer<>();

        qry.setLocalListener(lsnr);
        qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(new IgniteClosure<CacheEntryEvent<?, ?>, String>() {
            @Override public String apply(CacheEntryEvent<?, ?> evt) {
                return "" + evt.getKey() + evt.getValue();
            }
        }));

        QueryCursor<?> cur = clnNode.cache(DEFAULT_CACHE_NAME).query(qry);

        boolean first = true;

        int keyCnt = 1;

        for (int i = 0; i < 10; i++) {
            log.info("Start iteration: " + i);

            if (first)
                first = false;
            else {
                for (int srv = 0; srv < CLIENT_ID - 1; srv++)
                    startGrid(srv);
            }

            lsnr.latch = new CountDownLatch(keyCnt);

            for (int key = 0; key < keyCnt; key++)
                cache.apply().put(key, key);

            assertTrue("Failed to wait for event. Left events: " + lsnr.latch.getCount(),
                lsnr.latch.await(10, SECONDS));

            for (int srv = 0; srv < CLIENT_ID - 1; srv++)
                stopGrid(srv);
        }

        tryClose(cur);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformerException() throws Exception {
        try {
            startGrids(1);

            Ignite ignite = ignite(0);

            IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

            final CountDownLatch latch = new CountDownLatch(10);

            ContinuousQueryWithTransformer<Integer, Integer, Integer> qry = new ContinuousQueryWithTransformer<>();

            qry.setLocalListener(new EventListener<Integer>() {
                /** */
                @LoggerResource
                private IgniteLogger log;

                @Override public void onUpdated(Iterable<? extends Integer> evts) throws CacheEntryListenerException {
                    for (Integer evt : evts) {
                        if (log.isDebugEnabled())
                            log.debug("" + evt);
                    }
                }
            });

            qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(new IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Integer>, Integer>() {
                @Override public Integer apply(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
                    latch.countDown();

                    throw new RuntimeException("Test error.");
                }
            }));

            qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheEntryEventSerializableFilter<Integer, Integer>() {
                @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
                    return true;
                }
            }));

            try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
                for (int i = 0; i < 10; i++)
                    cache.put(i, i);

                assertTrue(latch.await(10, SECONDS));
            }
        } finally {
            stopAllGrids();
        }
    }

    /**
     * Ensure that every node see every update.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCrossCallback() throws Exception {
        startGrids(2);
        try {
            IgniteCache<Integer, Integer> cache1 = grid(0).cache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, Integer> cache2 = grid(1).cache(DEFAULT_CACHE_NAME);

            final int key1 = primaryKey(cache1);
            final int key2 = primaryKey(cache2);

            final CountDownLatch latch1 = new CountDownLatch(2);
            final CountDownLatch latch2 = new CountDownLatch(2);

            Factory<? extends IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Integer>, Integer>> factory =
                FactoryBuilder.factoryOf(
                    new IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Integer>, Integer>() {
                        @Override public Integer apply(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
                            return evt.getKey();
                        }
                    });

            ContinuousQueryWithTransformer<Integer, Integer, Integer> qry1 = new ContinuousQueryWithTransformer<>();

            qry1.setRemoteTransformerFactory(factory);

            qry1.setLocalListener(new EventListener<Integer>() {
                @Override public void onUpdated(Iterable<? extends Integer> evts) {
                    for (int evt : evts) {
                        log.info("Update in cache 1: " + evt);

                        if (evt == key1 || evt == key2)
                            latch1.countDown();
                    }
                }
            });

            ContinuousQueryWithTransformer<Integer, Integer, Integer> qry2 = new ContinuousQueryWithTransformer<>();

            qry2.setRemoteTransformerFactory(factory);

            qry2.setLocalListener(new EventListener<Integer>() {
                @Override public void onUpdated(Iterable<? extends Integer> evts) {
                    for (int evt : evts) {
                        log.info("Update in cache 2: " + evt);

                        if (evt == key1 || evt == key2)
                            latch2.countDown();
                    }
                }
            });

            try (QueryCursor<Cache.Entry<Integer, Integer>> ignored1 = cache2.query(qry1);
                 QueryCursor<Cache.Entry<Integer, Integer>> ignored2 = cache2.query(qry2)) {
                cache1.put(key1, key1);
                cache1.put(key2, key2);

                assertTrue(latch1.await(10, SECONDS));
                assertTrue(latch2.await(10, SECONDS));
            }
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @param cur Cur.
     */
    private void tryClose(QueryCursor<?> cur) {
        try {
            cur.close();
        }
        catch (Throwable e) {
            if (e instanceof IgniteClientDisconnectedException) {
                IgniteClientDisconnectedException ex = (IgniteClientDisconnectedException)e;

                ex.reconnectFuture().get();

                cur.close();
            }
            else
                throw e;
        }
    }

    /**
     */
    private static class CacheEventListener implements EventListener<String> {
        /** */
        public volatile CountDownLatch latch = new CountDownLatch(1);

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<? extends String> evts) {
            for (Object evt : evts) {
                log.info("Received cache event: " + evt);

                latch.countDown();
            }
        }
    }
}
