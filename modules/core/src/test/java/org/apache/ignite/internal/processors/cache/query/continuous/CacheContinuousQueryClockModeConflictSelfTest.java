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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test coverages case when update discarded
 */
public class CacheContinuousQueryClockModeConflictSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final int KEYS = 0;

    /** */
    public static final int ITERATION_CNT = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // Node should start exactly in the same order.
        startGrid(0);
        startGrid(1);
        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteLocalContinuousQuery() throws Exception {
        for (int i = 0; i < ITERATION_CNT; i++) {
            String cacheName = "test-cache-" + i;

            IgniteCache cache2 = ignite(2).createCache(cacheConfiguration(cacheName));

            try {
                IgniteCache cache0 = ignite(0).cache(cacheName);
                IgniteCache cache1 = ignite(1).cache(cacheName);

                final BlockingQueue<CacheEntryEvent<Integer, Integer>> evts0 = new LinkedBlockingQueue<>();
                QueryCursor query = registerQuery(cache0, evts0);

                final BlockingQueue<CacheEntryEvent<Integer, Integer>> evts1 = new LinkedBlockingQueue<>();
                QueryCursor query1 = registerQuery(cache1, evts1);

                final BlockingQueue<CacheEntryEvent<Integer, Integer>> evts2 = new LinkedBlockingQueue<>();
                QueryCursor query2 = registerQuery(cache2, evts2);

                Integer key = keyForNode(affinity(cache1), new AtomicInteger(0),
                    grid(0).cluster().localNode());

                // This update should CacheVersion with orderId == 3.
                cache2.put(key, 42);

                // The update will be have orderId == 2 and will be discarded.
                cache1.put(key, 41);

                assertEquals("Entry was updated.", 42, cache1.get(key));

                cache0.remove(key);

                assertNull("Entry was not removed.", cache1.get(key));

                CacheEntryEvent<Integer, Integer> e = evts0.poll(200, TimeUnit.MILLISECONDS);

                assertNotNull("Missed events.", e);
                assertEquals("Invalid event.", EventType.CREATED, e.getEventType());
                assertNull("Invalid event.", e.getOldValue());
                assertEquals("Invalid event.", Integer.valueOf(42), e.getValue());

                e = evts0.poll(200, TimeUnit.MILLISECONDS);

                assertNotNull("Missed events.", e);
                assertEquals("Invalid event.", EventType.REMOVED, e.getEventType());
                assertEquals("Invalid event.", Integer.valueOf(42), e.getOldValue());

                assertTrue("Received extra events.", evts1.isEmpty());
                assertTrue("Received extra events.", evts2.isEmpty());

                query.close();
                query1.close();
                query2.close();
            }
            finally {
                grid(0).destroyCache(cacheName);
            }
        }
    }

    private QueryCursor registerQuery(IgniteCache cache, final BlockingQueue<CacheEntryEvent<Integer, Integer>> evts) {
        ContinuousQuery qry = new ContinuousQuery();

        qry.setLocalListener(new CacheEntryUpdatedListener() {
            @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                for (Object o : iterable)
                    evts.add((CacheEntryEvent)o);
            }
        });

        qry.setLocal(true);

        return cache.query(qry);
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        return new CacheConfiguration(name)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.CLOCK)
            .setBackups(0);
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridNearAtomicFullUpdateRequest) {
                GridCacheVersion ver = ((GridNearAtomicFullUpdateRequest)msg0).updateVersion();

                ver.globalTime(142);
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
