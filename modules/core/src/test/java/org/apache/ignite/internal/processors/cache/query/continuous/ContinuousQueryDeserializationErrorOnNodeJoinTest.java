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

import java.util.concurrent.ThreadLocalRandom;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks, that errors during CQ remote filter deserialization doesn't prevent a node from joining a cluster.
 */
public class ContinuousQueryDeserializationErrorOnNodeJoinTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);
        ((TcpDiscoverySpi)igniteCfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        igniteCfg.setPeerClassLoadingEnabled(false);

        return igniteCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeserializationErrorOnJoiningNode() throws Exception {
        String cacheName = "cache";
        int recordsNum = 1000;

        Ignite node1 = startGrid(1);

        IgniteCache<Integer, Integer> cacheNode1 = node1.getOrCreateCache(cacheName);

        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();
        qry.setLocalListener((evt) -> log.info("Event received: " + evt));
        qry.setRemoteFilterFactory(remoteFilterFactory());

        cacheNode1.query(qry);

        // Deserialization error will happen, when the new node tries to deserialize the discovery data.
        Ignite node2 = startGrid(2);

        awaitPartitionMapExchange();

        // Check, that node and cache are functional.
        IgniteCache<Integer, Integer> cacheNode2 = node2.cache(cacheName);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < recordsNum; i++) {
            IgniteCache<Integer, Integer> cache =
                rnd.nextBoolean() ? cacheNode1 : cacheNode2;

            cache.put(i, i);
        }

        for (int i = 0; i < recordsNum; i++) {
            assertEquals(new Integer(i), cacheNode1.get(i));
            assertEquals(new Integer(i), cacheNode2.get(i));
        }
    }

    /**
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Remote filter.
     * @throws ReflectiveOperationException In case of instantiation failure.
     */
    private <K, V> Factory<? extends CacheEntryEventFilter<K, V>> remoteFilterFactory()
        throws ReflectiveOperationException {
        final Class<Factory<CacheEntryEventFilter>> evtFilterFactoryCls =
            (Class<Factory<CacheEntryEventFilter>>)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilterFactory");

        return (Factory<? extends CacheEntryEventFilter<K, V>>)(Object)evtFilterFactoryCls.newInstance();
    }
}
