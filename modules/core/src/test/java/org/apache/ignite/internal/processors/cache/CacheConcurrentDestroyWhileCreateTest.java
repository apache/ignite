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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.query.schema.SchemaExchangeWorkerTask;
import org.apache.ignite.internal.processors.query.schema.message.SchemaAbstractDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Checks that asynchronous exchange processing cause no problems at cache concurrent destroy right after creation.
 */
public class CacheConcurrentDestroyWhileCreateTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        try {
            final IgniteEx ignite = startGrid(0);

            final CountDownLatch delayLatch = new CountDownLatch(1);
            final CountDownLatch createLatch = new CountDownLatch(1);
            final CountDownLatch destroyLatch = new CountDownLatch(1);

            final GridWorker exchangeWorker =
                GridTestUtils.getFieldValue(
                    ignite.context().cache().context().exchange(),
                    GridCachePartitionExchangeManager.class,
                    "exchWorker");

            Assert.assertNotNull("Seems, code refactored! Please update test.", exchangeWorker);

            GridTestUtils.invoke(exchangeWorker, "addCustomTask", new ExchangeWorkerDelay(delayLatch));

            new Thread() {
                @Override public void run() {
                    new Thread() {
                        @Override public void run() {
                            waitForCacheDesctiptor(ignite, DEFAULT_CACHE_NAME, true);

                            new Thread() {
                                @Override public void run() {
                                    waitForCacheDesctiptor(ignite, DEFAULT_CACHE_NAME, false);

                                    delayLatch.countDown();
                                }
                            }.start();

                            ignite.destroyCache(DEFAULT_CACHE_NAME);

                            destroyLatch.countDown();
                        }
                    }.start();

                    ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

                    createLatch.countDown();
                }
            }.start();

            Assert.assertTrue(
                delayLatch.await(2000, TimeUnit.MILLISECONDS));

            Assert.assertTrue("Cache creation is not successful.",
                createLatch.await(2000, TimeUnit.MILLISECONDS));

            Assert.assertTrue("Cache destroy is not successful.",
                destroyLatch.await(2000, TimeUnit.MILLISECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testWithPublicAPI() throws Exception {
        try {
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
                    ignite2.getOrCreateCache(DEFAULT_CACHE_NAME);
                }
            }.start();

            latch.await();

            U.sleep(100); // large enough to start cache creation on ignite2

            new Thread() {
                @Override public void run() {
                    ignite3.destroyCache(DEFAULT_CACHE_NAME);
                }
            }.start();

            awaitPartitionMapExchange();
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite IgniteEx instance
     * @param cacheName Name of cache
     * @param exist Exist
     */
    private void waitForCacheDesctiptor(IgniteEx ignite, String cacheName, boolean exist) {
        while (true) {
            DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheName);

            if (exist ? desc != null : desc == null)
                return;
        }
    }

    /**
     * Custom exchange worker task implementation for delaying exchange worker processing.
     */
    static class ExchangeWorkerDelay extends SchemaExchangeWorkerTask implements CachePartitionExchangeWorkerTask {
        /** */
        private final CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        ExchangeWorkerDelay(CountDownLatch latch) {
            super(new SchemaAbstractDiscoveryMessage(null) {
                @Override public boolean exchange() {
                    return false;
                }

                @Nullable @Override public DiscoveryCustomMessage ackMessage() {
                    return null;
                }

                @Override public boolean isMutable() {
                    return false;
                }
            });

            this.latch = latch;
        }

        /** {@inheritDoc} */
        public SchemaAbstractDiscoveryMessage message() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                fail("Thread interrupted.");
            }

            return super.message();
        }
    }
}