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
import org.jetbrains.annotations.NotNull;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * Continuous queries execute in primary node tests.
 */
public class CacheContinuousQueryExecuteInPrimaryTest extends GridCommonAbstractTest
    implements Serializable {

    /** Name of cache */
    protected static final String CACHE_NAME = "test_cache";

    /** Latch timeout. */
    protected static final long LATCH_TIMEOUT = 5000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(true);
        cfg.setCacheConfiguration(cacheConfiguration());
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    @NotNull
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithoutEventsEntries() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).cache(CACHE_NAME);

        int ITERATION_CNT = 100;

        for (int i = 0; i < ITERATION_CNT; i++) {
            // Create new continuous query.
            ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

            qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> iterable)
                    throws CacheEntryListenerException {
                    assert false;
                }
            });

            // This filters return always false
            qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(
                new CacheEntryEventSerializableFilter<Integer, String>() {
                    @Override public boolean evaluate(
                        CacheEntryEvent<? extends Integer, ? extends String> cacheEntryEvent)
                        throws CacheEntryListenerException {
                        return false;
                    }
                }));

            // Execute query.
            try (QueryCursor<Cache.Entry<Integer, String>> qryCursor = cache.query(qry)) {
                for (int key = 0; key < 8; key++)
                    cache.put(key, Integer.toString(key));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithEventsEntries() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).cache(CACHE_NAME);

        // Create new continuous query.
        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

        final CountDownLatch latch = new CountDownLatch(4);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> iterable)
                throws CacheEntryListenerException {
                latch.countDown();
            }
        });

        // This filters return always false
        qry.setRemoteFilter(new CacheEntryEventSerializableFilter<Integer, String>() {
            @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e)
                throws CacheEntryListenerException {
                return e.getKey() % 2 == 0;
            }
        });

        // Execute query.
        cache.query(qry);

        for (int i = 0; i < 8; i++)
            cache.put(i, Integer.toString(i));

        assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }
}
