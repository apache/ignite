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

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.custom.DummyEventFilterFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Throws NPE or AssertionError when peer class loading enabled.
 */
public class ContinuousQueryPeerClassLoadingTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(!gridName.endsWith("0"));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteFilter() throws Exception {
        final Ignite srv = startGrid(0);

        final IgniteCache<Integer, String> cache = srv.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, String.valueOf(i));

        final Ignite client1 = startGrid(1);

        final ContinuousQuery<Integer, String> qry1 = new ContinuousQuery<>();
        final ContinuousQuery<Integer, String> qry2 = new ContinuousQuery<>();

        qry1.setRemoteFilterFactory(new DummyEventFilterFactory());
        qry2.setRemoteFilterFactory(new DummyEventFilterFactory());

        qry1.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
            @Override public void onUpdated(
                final Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) throws CacheEntryListenerException {
                System.out.println(">> Client 1 events " + evts);
            }
        });

        qry2.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
            @Override public void onUpdated(
                final Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) throws CacheEntryListenerException {
                System.out.println(">> Client 2 events " + evts);
            }
        });

        final IgniteCache<Integer, String> cache1 = client1.cache(CACHE_NAME);

        cache1.query(qry1);

        for (int i = 10; i < 20; i++)
            cache.put(i, String.valueOf(i));

        // Fail on start second client.
        final Ignite client2 = startGrid(2);

        final IgniteCache<Integer, String> cache2 = client2.cache(CACHE_NAME);

        cache2.query(qry2);

        for (int i = 20; i < 30; i++)
            cache.put(i, String.valueOf(i));
    }

}
