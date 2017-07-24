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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class CacheCreateInitDestroyRaceTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder();

    /** */
    private static final int CACHE_COUNT_LOW_THRESHOLD = 5;

    /** */
    private static final String CHECKPOINT_CACHE_NAME = "myCache";

    /** */
    private static final long NOISE_STABILIZATION_TERM_MS = 5000L;

    static {
        IP_FINDER.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
    }

    /** {@inheritDoc}*/
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setPeerClassLoadingEnabled(false);

        // This mojo prevents communication between nodes via shared memory, let 'em behave as in real network
        if (cfg.getCommunicationSpi() instanceof TcpCommunicationSpi) {
            ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);
        }

        return cfg;
    }

    /**
     * Tests for stable behavior on fast create-destroy sequence for the same cache in a cluster.
     * Non-atomic multi-step cache initialization could fail sporadically.
     *
     * @throws Exception if failed
     */
    public void testCacheCreateInitDestroyRace() throws Exception {
        Ignition.setClientMode(false);
        startGrid(0);
        final Ignite ignite2 = startGrid(1);
        final Ignite ignite3 = startGrid(2);

        Ignition.setClientMode(true);
        final Ignite igniteClient = startGrid(3);

        final Set<String> existingCacheNames = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        // This thread generates "noise" to keep future queue in partition-exchanger filled
        new Thread(new Runnable() {
            @Override public void run() {
                while (!existingCacheNames.contains(CHECKPOINT_CACHE_NAME)) {
                    try {
                        if (existingCacheNames.size() < CACHE_COUNT_LOW_THRESHOLD) {
                            final String cacheName = UUID.randomUUID().toString();
                            new Thread(new Runnable() {
                                @Override public void run() {
                                    igniteClient.getOrCreateCache(cacheName);
                                    existingCacheNames.add(cacheName);
                                    igniteClient.destroyCache(cacheName);
                                    existingCacheNames.remove(cacheName);
                                }
                            }).start();
                        }
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }).start();

        Thread.sleep(NOISE_STABILIZATION_TERM_MS);

        try {
            final CountDownLatch latch = new CountDownLatch(1);
            new Thread(new Runnable() {
                @Override public void run() {
                    existingCacheNames.add(CHECKPOINT_CACHE_NAME);
                    latch.countDown();
                    ignite2.getOrCreateCache(CHECKPOINT_CACHE_NAME);
                }
            }).start();
            latch.await();
            Thread.sleep(100); // large enough to start cache creation on ignite2
            new Thread(new Runnable() {
                @Override public void run() {
                    ignite3.destroyCache(CHECKPOINT_CACHE_NAME);
                }
            }).start();
        } catch (InterruptedException ignored) {}

        awaitPartitionMapExchange();

        stopAllGrids();
    }
}
