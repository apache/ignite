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

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Continuous queries execute in primary node tests.
 */
public class CacheContinuousQueryExecuteInPrimaryTest extends GridCommonAbstractTest
    implements Serializable {

    /** Latch timeout. */
    protected static final long LATCH_TIMEOUT = 5000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    @NotNull
    protected CacheConfiguration<Integer, String> cacheConfiguration(
        CacheAtomicityMode cacheAtomicityMode,
        CacheMode cacheMode) {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalCache() throws Exception {
        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(ATOMIC, LOCAL);

        doTestWithoutEventsEntries(ccfg);
        doTestWithEventsEntries(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedCache() throws Exception {
        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(ATOMIC, REPLICATED);

        doTestWithoutEventsEntries(ccfg);
        doTestWithEventsEntries(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedCache() throws Exception {
        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(ATOMIC, PARTITIONED);

        doTestWithoutEventsEntries(ccfg);
        doTestWithEventsEntries(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionLocalCache() throws Exception {
        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(TRANSACTIONAL, LOCAL);

        doTestWithoutEventsEntries(ccfg);
        doTestWithEventsEntries(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionReplicatedCache() throws Exception {
        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(TRANSACTIONAL, REPLICATED);

        doTestWithoutEventsEntries(ccfg);
        doTestWithEventsEntries(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionPartitionedCache() throws Exception {
        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(TRANSACTIONAL, PARTITIONED);

        doTestWithoutEventsEntries(ccfg);
        doTestWithEventsEntries(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestWithoutEventsEntries(CacheConfiguration<Integer, String> ccfg) throws Exception {

        try (IgniteCache<Integer, String> cache = grid(0).createCache(ccfg)) {

            int ITERATION_CNT = 100;
            final AtomicBoolean noOneListen = new AtomicBoolean(true);

            for (int i = 0; i < ITERATION_CNT; i++) {
                ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                    @Override public void onUpdated(
                        Iterable<CacheEntryEvent<? extends Integer, ? extends String>> iterable)
                        throws CacheEntryListenerException {
                        noOneListen.set(false);
                    }
                });

                qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(
                    new CacheEntryEventSerializableFilter<Integer, String>() {
                        @Override public boolean evaluate(
                            CacheEntryEvent<? extends Integer, ? extends String> cacheEntryEvent)
                            throws CacheEntryListenerException {
                            return false;
                        }
                    }));

                executeQuery(cache, qry, ccfg.getAtomicityMode() == TRANSACTIONAL);
            }

            assertTrue(noOneListen.get());

        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    private void executeQuery(IgniteCache<Integer, String> cache, ContinuousQuery<Integer, String> qry,
        boolean isTransactional) {
        try (QueryCursor<Cache.Entry<Integer, String>> qryCursor = cache.query(qry)) {
            Transaction tx = null;

            if (isTransactional)
                tx = cache.unwrap(Ignite.class).transactions().txStart();

            try {
                for (int key = 0; key < 8; key++)
                    cache.put(key, Integer.toString(key));

                Map<Integer, String> map = new HashMap<>(8);

                for (int key = 8; key < 16; key++)
                    map.put(key, Integer.toString(key));

                cache.putAll(map);

                if (isTransactional)
                    tx.commit();

            }
            finally {
                if (isTransactional)
                    tx.close();
            }

            for (int key = 0; key < 8; key++) {
                cache.invoke(key, new EntryProcessor<Integer, String, Object>() {
                    @Override public Object process(MutableEntry<Integer, String> entry,
                        Object... objects) throws EntryProcessorException {
                        entry.setValue(Integer.toString(entry.getKey() + 1));
                        return null;
                    }

                });
            }

            Map<Integer, EntryProcessor<Integer, String, Object>> invokeMap = new HashMap<>(8);

            for (int key = 8; key < 16; key++) {
                invokeMap.put(key, new EntryProcessor<Integer, String, Object>() {
                    @Override public Object process(MutableEntry<Integer, String> entry,
                        Object... objects) throws EntryProcessorException {
                        entry.setValue(Integer.toString(entry.getKey() - 1));

                        return null;
                    }
                });
            }

            cache.invokeAll(invokeMap);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void doTestWithEventsEntries(CacheConfiguration<Integer, String> ccfg) throws Exception {
        try (IgniteCache<Integer, String> cache = grid(0).createCache(ccfg)) {

            ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

            final CountDownLatch latch = new CountDownLatch(16);
            final AtomicInteger cnt = new AtomicInteger(0);

            qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> iterable)
                    throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends Integer, ? extends String> e : iterable) {
                        cnt.incrementAndGet();
                        latch.countDown();
                    }
                }
            });

            qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(
                new CacheEntryEventSerializableFilter<Integer, String>() {
                    @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e)
                        throws CacheEntryListenerException {
                        return e.getKey() % 2 == 0;
                    }
                }
            ));

            // Execute query.
            executeQuery(cache, qry, ccfg.getAtomicityMode() == TRANSACTIONAL);

            assertTrue(latch.await(LATCH_TIMEOUT, MILLISECONDS));
            assertEquals(16, cnt.get());
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }
}
