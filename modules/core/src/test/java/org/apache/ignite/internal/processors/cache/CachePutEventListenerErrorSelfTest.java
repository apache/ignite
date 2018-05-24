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

import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for cache put with error in event listener.
 */
public class CachePutEventListenerErrorSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        Ignition.setClientMode(true);

        try {
            Ignite ignite = startGrid("client");

            ignite.events().remoteListen(
                new IgniteBiPredicate<UUID, Event>() {
                    @Override public boolean apply(UUID uuid, Event evt) {
                        return true;
                    }
                },
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        throw new NoClassDefFoundError("XXX");
                    }
                },
                EventType.EVT_CACHE_OBJECT_PUT
            );
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedAtomicOnHeap() throws Exception {
        doTest(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTransactionalOnHeap() throws Exception {
        doTest(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedAtomicOnHeap() throws Exception {
        doTest(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTransactionalOnHeap() throws Exception {
        doTest(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void doTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode)
        throws Exception {
        Ignite ignite = grid("client");

        try {
            CacheConfiguration<Integer, Integer> cfg = defaultCacheConfiguration();

            cfg.setName("cache");
            cfg.setCacheMode(cacheMode);
            cfg.setAtomicityMode(atomicityMode);

            IgniteCache<Integer, Integer> cache = ignite.createCache(cfg);

            IgniteFuture f = cache.putAsync(0, 0);

            try {
                f.get(2000);

                assert false : "Exception was not thrown";
            }
            catch (CacheException e) {
                info("Caught expected exception: " + e);
            }
        }
        finally {
            ignite.destroyCache("cache");
        }
    }
}
