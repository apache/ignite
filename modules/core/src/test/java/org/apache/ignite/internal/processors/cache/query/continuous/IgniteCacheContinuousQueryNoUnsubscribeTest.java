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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheContinuousQueryNoUnsubscribeTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static AtomicInteger cntr = new AtomicInteger();

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setPeerClassLoadingEnabled(false);
        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();

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
    public void testNoUnsubscribe() throws Exception {
       checkNoUnsubscribe(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoUnsubscribeClient() throws Exception {
        checkNoUnsubscribe(true);
    }

    /**
     * @param client Client node flag.
     * @throws Exception If failed.
     */
    private void checkNoUnsubscribe(boolean client) throws Exception {
        cntr.set(0);

        this.client = client;

        try (Ignite ignite = startGrid(3)) {
            ContinuousQuery qry = new ContinuousQuery();

            qry.setLocalListener(new CacheEntryUpdatedListener() {
                @Override public void onUpdated(Iterable evts) {
                    // No-op.
                }
            });

            qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(CacheTestRemoteFilter.class));

            qry.setAutoUnsubscribe(false);

            ignite.cache(null).query(qry);

            ignite.cache(null).put(1, 1);

            assertEquals(1, cntr.get());
        }

        this.client = false;

        try (Ignite newSrv = startGrid(3)) {
            Integer key = primaryKey(newSrv.cache(null));

            newSrv.cache(null).put(key, 1);

            assertEquals(2, cntr.get());

            for (int i = 0; i < 10; i++)
                ignite(0).cache(null).put(i, 1);

            assertEquals(12, cntr.get());
        }

        for (int i = 10; i < 20; i++)
            ignite(0).cache(null).put(i, 1);

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
