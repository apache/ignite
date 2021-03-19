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
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class TcpDiscoveryPendingMessageDeliveryTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean blockMsgs;

    /** */
    private volatile boolean blockNodeAddedMsgs;

    private static CountDownLatch cdl = new CountDownLatch(1);

    private static CountDownLatch cdl1 = new CountDownLatch(1);

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
        else if (igniteInstanceName.startsWith("hang"))
            disco = new HangNodeAddedDiscoverySpi();
        else if (igniteInstanceName.startsWith("lcoord"))
            disco = new CoordDiscoverySpi();
        else
            disco = new TcpDiscoverySpi();

        if (igniteInstanceName.startsWith("client"))
            cfg.setClientMode(true);

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

    private static CountDownLatch lastCdl = new CountDownLatch(3);

    private static CountDownLatch cdlJoining = new CountDownLatch(1);

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test0() throws Exception {
        final TreeMap<Object, Object> map = new TreeMap<>();
        for (int i=0; i < 1000; i++)
            map.put(i, i);

        Ignite coord = startGrid("lcoord");
        CoordDiscoverySpi coordDiscoSpi = ((CoordDiscoverySpi)coord.configuration().getDiscoverySpi());

        IgniteEx node = startGrid("node1");

        Ignite client = startGrid("client");

        TcpDiscoverySpi cliDisco = (TcpDiscoverySpi)client.configuration().getDiscoverySpi();

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>("cache0");

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        client.createCache(ccfg);

        IgniteInternalFuture<IgniteEx> fut = GridTestUtils.runAsync(() -> startGrid("hang123"));

        lastCdl.await();

        blockNodeAddedMsgs = true;

        System.err.println("!!!!start destr");

        client.destroyCache(ccfg.getName());

/*        for (int i=0; i < 100; ++i) {
            //sendDummyCustomMessage(cliDisco, IgniteUuid.randomUuid());
            //client.destroyCache("cache0" + i);
            DistributedMetaStorageUpdateMessage dm = new DistributedMetaStorageUpdateMessage(UUID.randomUUID(), IgniteUuid.randomUuid().toString(), new byte[0]);
            cliDisco.sendCustomEvent(new CustomMessageWrapper(dm));
        }*/

        cdl.await();

        //cdlJoining.countDown();

/*        cache.put(1, 1);

        assertEquals(1, cache.get(1));*/

        blockNodeAddedMsgs = false;

        node.createCache(ccfg);

        IgniteCache<Object, Object> cache = client.cache(ccfg.getName());

        doInTransaction(node, OPTIMISTIC, SERIALIZABLE, new Callable<Void>() {
            @Override public Void call() {

                cache.putAll(map);

                return null;
            }
        });

        System.err.println("!!!!start get");

        for (int i = 0; i < 100; ++i)
            assertEquals(1, cache.get(1));
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

    private class CoordDiscoverySpi extends TcpDiscoverySpi {

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                if (msg.verified()) {
                    lastCdl.countDown();
                    System.err.println("node TcpDiscoveryNodeAddedMessage3");
                }
            }
            else if (msg.toString().contains("DynamicCacheChangeBatch"))
                System.err.println("node DynamicCacheChangeBatch: " + msg);
            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /** */
    private class HangNodeAddedDiscoverySpi extends TcpDiscoverySpi {
        Runnable sup;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                if (blockNodeAddedMsgs) {
                    TcpDiscoveryNodeAddFinishedMessage msg0 = (TcpDiscoveryNodeAddFinishedMessage) msg;
                    System.err.println("!!! catch1");
                    cdl.countDown();

                    super.writeToSocket(sock, msg, data, timeout);
                }
            }
            else
                super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                if (blockNodeAddedMsgs) {
                    TcpDiscoveryNodeAddFinishedMessage msg0 = (TcpDiscoveryNodeAddFinishedMessage) msg;
                    System.err.println("!!! catch2");
                    cdl.countDown();

                    super.writeToSocket(sock, msg, timeout);
                }
            }
            else
                super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                if (blockNodeAddedMsgs) {
                    TcpDiscoveryNodeAddFinishedMessage msg0 = (TcpDiscoveryNodeAddFinishedMessage) msg;
                    System.err.println("!!! catch3");
                    cdl.countDown();

                    super.writeToSocket(sock, out, msg, timeout);
                }
            }
            else
                super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                if (blockNodeAddedMsgs) {
                    TcpDiscoveryNodeAddFinishedMessage msg0 = (TcpDiscoveryNodeAddFinishedMessage) msg;
                    System.err.println("!!! catch4");
                    cdl.countDown();

                    super.writeToSocket(msg, sock, res, timeout);
                }
            }
            else
                super.writeToSocket(msg, sock, res, timeout);
        }

        TcpDiscoveryImpl discovery() {
            return impl;
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

