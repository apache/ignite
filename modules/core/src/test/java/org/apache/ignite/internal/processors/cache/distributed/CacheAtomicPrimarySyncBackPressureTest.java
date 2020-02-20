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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that back-pressure control restricts uncontrolled growing
 * of backup message queue. This means, if queue too big - any reads
 * will be stopped until received acks from backup nodes.
 */
public class CacheAtomicPrimarySyncBackPressureTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration("cache");

        ccfg.setBackups(1);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        ccfg.setNodeFilter(new NodeFilter());

        TestCommunicationSpi spi = new TestCommunicationSpi();

        spi.setMessageQueueLimit(100);

        cfg.setCommunicationSpi(spi);
        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientPut() throws Exception {
        Ignite srv1 = startGrid("server1");
        Ignite srv2 = startGrid("server2");

        final Ignite client = startClientGrid("client");

        checkBackPressure(client, srv1, srv2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerPut() throws Exception {
        Ignite srv1 = startGrid("server1");
        Ignite srv2 = startGrid("server2");

        final Ignite client = startGrid("server3");

        checkBackPressure(client, srv1, srv2);
    }

    /**
     * @param client Producer node.
     * @throws InterruptedException If failed.
     */
    private void checkBackPressure(Ignite client, final Ignite srv1, final Ignite srv2) throws Exception {
        final IgniteCache<Integer, String> cache = client.cache("cache");

        awaitPartitionMapExchange();

        for (int i = 0; i < 10000; i++) {
            cache.put(i, String.valueOf(i));

            if (i % 100 == 0) {
                int size1 = futuresNum(srv1);
                int size2 = futuresNum(srv2);

                assert size1 < 150 : size1;
                assert size2 < 150 : size2;
            }
        }
    }

    /**
     * @param ignite Ignite.
     * @return Size of the backup queue.
     */
    private int futuresNum(Ignite ignite) {
        return ((IgniteKernal)ignite).context().cache().context().mvcc().atomicFutures().size();
    }

    /**
     * Delays backup update acks.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtAtomicDeferredUpdateResponse)
                sleep(100);

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     * @param millis Millis.
     */
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            throw new IgniteSpiException(e);
        }
    }

    /**
     * Filters out server node producer.
     */
    private static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !("server3".equals(node.attribute("org.apache.ignite.ignite.name")));
        }
    }
}
