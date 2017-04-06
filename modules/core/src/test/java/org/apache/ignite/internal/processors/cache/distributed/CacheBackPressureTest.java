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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheBackPressureTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration("cache");

        ccfg.setBackups(1);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        ccfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        TestCommunicationSpi spi = new TestCommunicationSpi();

        spi.setMessageQueueLimit(100);

        cfg.setCommunicationSpi(spi);
        cfg.setClientMode(gridName.contains("client"));
        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    public void testClientPut() throws Exception {
        startGrid("server1");
        startGrid("server2");

        final Ignite client = startGrid("client");

        checkBackPressure(client);
    }

    public void testServerPut() throws Exception {
        startGrid("server1");
        startGrid("server2");
        final Ignite client = startGrid("server3");

        checkBackPressure(client);
    }

    private void checkBackPressure(Ignite client) throws InterruptedException {
        final IgniteCache<Integer, String> cache = client.cache("cache");

        awaitPartitionMapExchange();

        final IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 300; i++)
                    cache.put(i, String.valueOf(i));

                return null;
            }
        });

        // Because no acks received from backups, puts
        // will hang on back-pressure control.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fut.get(2_000);

                return null;
            }
        }, IgniteFutureTimeoutCheckedException.class, null);
    }

    /**
     * Drops backup update acks.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtAtomicDeferredUpdateResponse)
                return;

            super.sendMessage(node, msg, ackC);
        }
    }
}
