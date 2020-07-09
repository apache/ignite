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

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheContinuousQueryNoUnsubscribeTest extends GridCommonAbstractTest {
    /** */
    private static AtomicInteger cntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoUnsubscribe() throws Exception {
       checkNoUnsubscribe(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoUnsubscribeClient() throws Exception {
        checkNoUnsubscribe(true);
    }

    /**
     * @param client Client node flag.
     * @throws Exception If failed.
     */
    private void checkNoUnsubscribe(boolean client) throws Exception {
        cntr.set(0);

        try (Ignite ignite = client ? startClientGrid(3) : startGrid(3)) {
            ContinuousQuery qry = new ContinuousQuery();

            qry.setLocalListener(new CacheEntryUpdatedListener() {
                @Override public void onUpdated(Iterable evts) {
                    // No-op.
                }
            });

            qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(CacheTestRemoteFilter.class));

            qry.setAutoUnsubscribe(false);

            ignite.cache(DEFAULT_CACHE_NAME).query(qry);

            ignite.cache(DEFAULT_CACHE_NAME).put(1, 1);

            assertEquals(1, cntr.get());
        }

        try (Ignite newSrv = startGrid(3)) {
            awaitPartitionMapExchange();

            Integer key = primaryKey(newSrv.cache(DEFAULT_CACHE_NAME));

            newSrv.cache(DEFAULT_CACHE_NAME).put(key, 1);

            assertEquals(2, cntr.get());

            for (int i = 0; i < 10; i++)
                ignite(0).cache(DEFAULT_CACHE_NAME).put(i, 1);

            assertEquals(12, cntr.get());
        }

        for (int i = 10; i < 20; i++)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, 1);

        assertEquals(22, cntr.get());
    }

    /**
     *
     */
    public static class CacheTestRemoteFilter implements CacheEntryEventSerializableFilter<Object, Object> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) throws CacheEntryListenerException {
            cntr.incrementAndGet();

            return true;
        }
    }
}
