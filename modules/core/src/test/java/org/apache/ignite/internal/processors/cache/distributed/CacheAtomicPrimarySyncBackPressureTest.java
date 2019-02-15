/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Checks that back-pressure control restricts uncontrolled growing
 * of backup message queue. This means, if queue too big - any reads
 * will be stopped until received acks from backup nodes.
 */
@RunWith(JUnit4.class)
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
        cfg.setClientMode(gridName.contains("client"));
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

        final Ignite client = startGrid("client");

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
