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

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class TcpDiscoveryPendingMessageDeliveryTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean blockMsgs;

    /** */
    private Set<TcpDiscoveryAbstractMessage> receivedEnsuredMsgs;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        blockMsgs = false;
        receivedEnsuredMsgs = new GridConcurrentHashSet<>();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco;

        if (igniteInstanceName.startsWith("victim"))
            disco = new DyingDiscoverySpi();
        else if (igniteInstanceName.startsWith("listener"))
            disco = new ListeningDiscoverySpi();
        else if (igniteInstanceName.startsWith("receiver"))
            disco = new DyingThreadDiscoverySpi();
        else
            disco = new TcpDiscoverySpi();

        disco.setIpFinder(sharedStaticIpFinder);
        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPendingMessagesOverflow() throws Exception {
        Ignite coord = startGrid("coordinator");
        TcpDiscoverySpi coordDisco = (TcpDiscoverySpi)coord.configuration().getDiscoverySpi();

        Set<TcpDiscoveryAbstractMessage> sentEnsuredMsgs = new GridConcurrentHashSet<>();
        coordDisco.addSendMessageListener(msg -> {
            if (coordDisco.ensured(msg))
                sentEnsuredMsgs.add(msg);
        });

        // Victim doesn't send acknowledges, so we need an intermediate node to accept messages,
        // so the coordinator could mark them as pending.
        Ignite mediator = startGrid("mediator");

        Ignite victim = startGrid("victim");

        startGrid("listener");

        sentEnsuredMsgs.clear();
        receivedEnsuredMsgs.clear();

        // Initial custom message will travel across the ring and will be discarded.
        sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        assertTrue("Sent: " + sentEnsuredMsgs + "; received: " + receivedEnsuredMsgs,
            GridTestUtils.waitForCondition(() -> {
                log.info("Waiting for messages delivery");

                return receivedEnsuredMsgs.equals(sentEnsuredMsgs);
            }, 10000));

        blockMsgs = true;

        log.info("Sending dummy custom messages");

        // Non-discarded messages shouldn't be dropped from the queue.
        int msgsNum = 2000;

        for (int i = 0; i < msgsNum; i++)
            sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        mediator.close();
        victim.close();

        assertTrue("Sent: " + sentEnsuredMsgs + "; received: " + receivedEnsuredMsgs,
            GridTestUtils.waitForCondition(() -> {
                log.info("Waiting for messages delivery [sentSize=" + sentEnsuredMsgs.size() + ", rcvdSize=" + receivedEnsuredMsgs.size() + ']');

                return receivedEnsuredMsgs.equals(sentEnsuredMsgs);
            }, 10000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomMessageInSingletonCluster() throws Exception {
        Ignite coord = startGrid("coordinator");
        TcpDiscoverySpi coordDisco = (TcpDiscoverySpi)coord.configuration().getDiscoverySpi();

        Set<TcpDiscoveryAbstractMessage> sentEnsuredMsgs = new GridConcurrentHashSet<>();
        coordDisco.addSendMessageListener(msg -> {
            if (coordDisco.ensured(msg))
                sentEnsuredMsgs.add(msg);
        });

        // Custom message on a singleton cluster shouldn't break consistency of PendingMessages.
        sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        // Victim doesn't send acknowledges, so we need an intermediate node to accept messages,
        // so the coordinator could mark them as pending.
        Ignite mediator = startGrid("mediator");

        Ignite victim = startGrid("victim");

        startGrid("listener");

        sentEnsuredMsgs.clear();
        receivedEnsuredMsgs.clear();

        blockMsgs = true;

        log.info("Sending dummy custom messages");

        // Non-discarded messages shouldn't be dropped from the queue.
        int msgsNum = 100;

        for (int i = 0; i < msgsNum; i++)
            sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        mediator.close();
        victim.close();

        assertTrue("Sent: " + sentEnsuredMsgs + "; received: " + receivedEnsuredMsgs,
            GridTestUtils.waitForCondition(() -> {
                log.info("Waiting for messages delivery");

                return receivedEnsuredMsgs.equals(sentEnsuredMsgs);
            }, 10000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeliveryAllFailedMessagesInCorrectOrder() throws Exception {
        IgniteEx coord = startGrid("coordinator");
        TcpDiscoverySpi coordDisco = (TcpDiscoverySpi)coord.configuration().getDiscoverySpi();

        Set<TcpDiscoveryAbstractMessage> sentEnsuredMsgs = new GridConcurrentHashSet<>();
        coordDisco.addSendMessageListener(msg -> {
            if (coordDisco.ensured(msg))
                sentEnsuredMsgs.add(msg);
        });

        //Node which receive message but will not send it further around the ring.
        IgniteEx receiver = startGrid("receiver");

        //Node which will be failed first.
        IgniteEx dummy = startGrid("dummy");

        //Node which should received all fail message in any way.
        startGrid("listener");

        sentEnsuredMsgs.clear();
        receivedEnsuredMsgs.clear();

        blockMsgs = true;

        log.info("Sending fail node messages");

        coord.context().discovery().failNode(dummy.localNode().id(), "Dummy node failed");
        coord.context().discovery().failNode(receiver.localNode().id(), "Receiver node failed");

        boolean delivered = GridTestUtils.waitForCondition(() -> {
            log.info("Waiting for messages delivery");

            return receivedEnsuredMsgs.equals(sentEnsuredMsgs);
        }, 5000);

        assertTrue("Sent: " + sentEnsuredMsgs + "; received: " + receivedEnsuredMsgs, delivered);
    }

    /**
     * @param disco Discovery SPI.
     * @param id Message id.
     */
    private void sendDummyCustomMessage(TcpDiscoverySpi disco, IgniteUuid id) {
        disco.sendCustomEvent(new CustomMessageWrapper(new DummyCustomDiscoveryMessage(id)));
    }

    /**
     * Discovery SPI, that makes a thread to die when {@code blockMsgs} is set to {@code true}.
     */
    private class DyingThreadDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (blockMsgs)
                throw new RuntimeException("Thread is dying");
        }
    }

    /**
     * Discovery SPI, that makes a node stop sending messages when {@code blockMsgs} is set to {@code true}.
     */
    private class DyingDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (!blockMsgs)
                super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (!blockMsgs)
                super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (!blockMsgs)
                super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (!blockMsgs)
                super.writeToSocket(msg, sock, res, timeout);
        }
    }

    /**
     *
     */
    private class ListeningDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (ensured(msg))
                receivedEnsuredMsgs.add(msg);
        }
    }

    /**
     *
     */
    private static class DummyCustomDiscoveryMessage implements DiscoveryCustomMessage {
        /** */
        private final IgniteUuid id;

        /**
         * @param id Message id.
         */
        DummyCustomDiscoveryMessage(IgniteUuid id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
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
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }
    }
}

