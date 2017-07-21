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

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.query.schema.SchemaExchangeWorkerTask;
import org.apache.ignite.internal.processors.query.schema.message.SchemaAbstractDiscoveryMessage;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheInitOnCoordinatorFailureTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateAndDestroyCaches() throws Exception {
        final IgniteEx ignite = startGrid(0);

        final GridWorker exchangeWorker =
            GridTestUtils.getFieldValue(
                ignite.context().cache().context().exchange(),
                GridCachePartitionExchangeManager.class,
                "exchWorker");

        GridTestUtils.invoke(exchangeWorker, "addCustomTask", new ExchangeWorkerDelay(1000));

        new Thread() {
            final String cacheName = "cache";

            @Override public void run() {
                new Thread() {
                    @Override public void run() {
                        uncheckedSleep(500);

                        ignite.destroyCache(cacheName);
                    }
                }.start();

                ignite.getOrCreateCache(cacheName);
            }
        }.start();

        uncheckedSleep(2000);

        assert !exchangeWorker.isDone();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param millis Milliseconds to sleep.
     * @throws IgniteInterruptedException Wrapped {@link InterruptedException}.
     */
    private static void uncheckedSleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException(e);
        }
    }

    /**
     * Custom exchange worker task implementation for delaying exchange worker processing.
     */
    static class ExchangeWorkerDelay extends SchemaExchangeWorkerTask implements CachePartitionExchangeWorkerTask {
        /** */
        private final long millis;

        /**
         * @param millis Milliseconds for delay.
         */
        ExchangeWorkerDelay(final long millis) {
            super(new SchemaAbstractDiscoveryMessage(null) {
                /** {@inheritDoc} */
                @Override public boolean exchange() {
                    return false;
                }

                /** {@inheritDoc} */
                @Nullable @Override public DiscoveryCustomMessage ackMessage() {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public boolean isMutable() {
                    return false;
                }

                /** {@inheritDoc} */
                @Override public String toString() {
                    return "Exchange worker was delayed for " + millis + " ms.";
                }
            });

            this.millis = millis;
        }

        /** {@inheritDoc} */
        public SchemaAbstractDiscoveryMessage message() {
            uncheckedSleep(millis);

            return super.message();
        }
    }
}
