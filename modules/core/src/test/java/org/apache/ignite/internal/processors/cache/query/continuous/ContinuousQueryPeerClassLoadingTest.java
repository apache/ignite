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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Checks if filter factory correctly deployed on all nodes.
 */
public class ContinuousQueryPeerClassLoadingTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(gridName.contains("client"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteFilterFactoryClient() throws Exception {
        check("server", "client1", "client2");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteFilterFactoryServer1() throws Exception {
        check("server1", "server2", "client");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteFilterFactoryServer2() throws Exception {
        check("server1", "server2", "server3");
    }

    /**
     * @param node1Name Node 1 name.
     * @param node2Name Node 2 name.
     * @param node3Name Node 3 name.
     */
    private void check(String node1Name, String node2Name, String node3Name) throws Exception {
        final Ignite node1 = startGrid(node1Name);

        final IgniteCache<Integer, String> cache = node1.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, String.valueOf(i));

        final Ignite node2 = startGrid(node2Name);

        final ContinuousQuery<Integer, String> qry1 = new ContinuousQuery<>();
        final ContinuousQuery<Integer, String> qry2 = new ContinuousQuery<>();

        qry1.setRemoteFilterFactory(new DummyEventFilterFactory<>());
        qry2.setRemoteFilterFactory(new DummyEventFilterFactory<>());

        final AtomicInteger client1Evts = new AtomicInteger(0);
        final AtomicInteger client2Evts = new AtomicInteger(0);

        final CountDownLatch latch1 = new CountDownLatch(20);
        final CountDownLatch latch2 = new CountDownLatch(10);

        qry1.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
            @Override public void onUpdated(
                final Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) throws CacheEntryListenerException {
                System.out.println(">> Client 1 events " + evts);
                for (CacheEntryEvent<? extends Integer, ? extends String> evt : evts)
                    latch1.countDown();
            }
        });

        qry2.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
            @Override public void onUpdated(
                final Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) throws CacheEntryListenerException {
                System.out.println(">> Client 2 events " + evts);
                for (CacheEntryEvent<? extends Integer, ? extends String> evt : evts)
                    latch2.countDown();
            }
        });

        final IgniteCache<Integer, String> cache1 = node2.cache(CACHE_NAME);

        cache1.query(qry1);

        for (int i = 10; i < 20; i++)
            cache.put(i, String.valueOf(i));

        // Fail on start second client.
        final Ignite node3 = startGrid(node3Name);

        final IgniteCache<Integer, String> cache2 = node3.cache(CACHE_NAME);

        cache2.query(qry2);

        for (int i = 20; i < 30; i++)
            cache.put(i, String.valueOf(i));

        assert latch1.await(5, TimeUnit.SECONDS) : latch1.getCount();
        assert latch2.await(5, TimeUnit.SECONDS) : latch2.getCount();
    }

}
