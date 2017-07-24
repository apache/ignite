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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.query.schema.SchemaExchangeWorkerTask;
import org.apache.ignite.internal.processors.query.schema.message.SchemaAbstractDiscoveryMessage;
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