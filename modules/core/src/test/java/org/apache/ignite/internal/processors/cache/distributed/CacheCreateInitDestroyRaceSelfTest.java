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
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Ensures stable behavior on fast create-destroy sequence for the same cache in a cluster.
 */
public class CacheCreateInitDestroyRaceSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CHECKPOINT_CACHE_NAME = "myCache";

    /**
     * @throws Exception if failed.
     */
    public void testCacheCreateInitDestroyRace() throws Exception {
        Ignition.setClientMode(false);

        startGrid(0);
        final Ignite ignite2 = startGrid(1);
        final Ignite ignite3 = startGrid(2);

        Ignition.setClientMode(true);

        final Ignite igniteClient = startGrid(3);

        final Set<String> existingCacheNames = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        final CountDownLatch noiseLatch = new CountDownLatch(1);

        // This thread generates "noise" to keep future queue in partition-exchanger filled
        final Thread noiseGenerator = new Thread() {
            @Override public void run() {
                final int cacheCountLowThreshold = 5;
                final int cacheCountHighThreshold = 10;

                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        int cacheCount = existingCacheNames.size();

                        if (cacheCount < cacheCountLowThreshold) {
                            final String cacheName = UUID.randomUUID().toString();

                            new Thread() {
                                @Override public void run() {
                                    igniteClient.getOrCreateCache(cacheName);

                                    existingCacheNames.add(cacheName);
                                }
                            }.start();
                        }
                        else if (cacheCount > cacheCountHighThreshold) {
                            noiseLatch.countDown();

                            new Thread() {
                                @Override public void run() {
                                    try {
                                        String cacheNameToRemove = existingCacheNames.iterator().next();

                                        existingCacheNames.remove(cacheNameToRemove);

                                        igniteClient.destroyCache(cacheNameToRemove);
                                    }
                                    catch (Exception ignored) {
                                    }
                                }
                            }.start();
                        }

                        U.sleep(10);
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                    }
                }
            }
        };

        noiseGenerator.start();

        noiseLatch.await();

        final CountDownLatch latch = new CountDownLatch(1);

        new Thread() {
            @Override public void run() {
                noiseGenerator.interrupt();
                latch.countDown();
                ignite2.getOrCreateCache(CHECKPOINT_CACHE_NAME);
            }
        }.start();

        latch.await();

        U.sleep(100); // large enough to start cache creation on ignite2

        new Thread() {
            @Override public void run() {
                ignite3.destroyCache(CHECKPOINT_CACHE_NAME);
            }
        }.start();

        awaitPartitionMapExchange();

        stopAllGrids();
    }
}
