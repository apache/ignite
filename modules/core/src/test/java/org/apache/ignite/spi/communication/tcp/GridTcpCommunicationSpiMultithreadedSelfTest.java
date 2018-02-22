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

package org.apache.ignite.spi.communication.tcp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Class for multithreaded {@link TcpCommunicationSpi} test.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public class GridTcpCommunicationSpiMultithreadedSelfTest extends GridSpiAbstractTest<TcpCommunicationSpi> {
    /** Connection idle timeout */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** Thread count for testFlowSend test. */
    public static final int THREAD_CNT = 20;

    /** Message id sequence. */
    private AtomicLong msgId = new AtomicLong();

    /** */
    private final boolean useShmem;

    /** SPI resources. */
    private static final Collection<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /** SPIs */
    private static final Map<UUID, CommunicationSpi<Message>> spis = new ConcurrentHashMap<>();

    /** Listeners. */
    private static final Map<UUID, MessageListener> lsnrs = new HashMap<>();

    /** Initialized nodes */
    private static final List<ClusterNode> nodes = new ArrayList<>();

    /** */
    private static GridTimeoutProcessor timeoutProcessor;

    /** Flag indicating if listener should reject messages. */
    private static boolean reject;

    static {
        GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new GridTestMessage();
            }
        });
    }

    /**
     * @param useShmem Use shared mem.
     */
    GridTcpCommunicationSpiMultithreadedSelfTest(boolean useShmem) {
        super(false);

        this.useShmem = useShmem;
    }

    /**
     *
     */
    public GridTcpCommunicationSpiMultithreadedSelfTest() {
        this(false);
    }

    /**
     * Accumulating listener.
     */
    @SuppressWarnings({"deprecation"})
    private static class MessageListener implements CommunicationListener<Message> {
        /** Node id of local node. */
        private final UUID locNodeId;

        /** Received messages by node. */
        private ConcurrentLinkedDeque8<GridTestMessage> rcvdMsgs = new ConcurrentLinkedDeque8<>();

        /** Count of messages received from remote nodes */
        private AtomicInteger rmtMsgCnt = new AtomicInteger();

        /**
         * @param locNodeId Local node ID.
         */
        MessageListener(UUID locNodeId) {
            assert locNodeId != null;

            this.locNodeId = locNodeId;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            msgC.run();

            if (msg instanceof GridTestMessage) {
                GridTestMessage testMsg = (GridTestMessage)msg;

                if (!testMsg.getSourceNodeId().equals(nodeId))
                    fail("Listener nodeId is not equal to message nodeId.");

                if (!reject)
                    rcvdMsgs.offer(testMsg);

                if (!locNodeId.equals(nodeId))
                    rmtMsgCnt.incrementAndGet();
            }
            else
                fail();
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }

        /**
         * @return Queue containing received messages in receive order.
         */
        public ConcurrentLinkedDeque8<GridTestMessage> receivedMsgs() {
            return rcvdMsgs;
        }

        /**
         * @return Count of messages received from remote node.
         */
        public int remoteMessageCount() {
            return rmtMsgCnt.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MessageListener [nodeId=" + locNodeId + ", rcvd=" + rcvdMsgs.sizex() + ']';
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendToRandomNodesMultithreaded() throws Exception {
        info(">>> Starting send to random nodes multithreaded test. <<<");

        reject = false;

        assertEquals("Invalid listener count", getSpiCount(), lsnrs.size());

        final ConcurrentMap<UUID, ConcurrentLinkedDeque8<GridTestMessage>> msgs = new ConcurrentHashMap<>();

        final int iterationCnt = 5000;

        long start = System.currentTimeMillis();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            /** Randomizer. */
            private Random rnd = new Random();

            @Override public void run() {
                try {
                    for (int i = 0; i < iterationCnt; i++) {
                        ClusterNode from = randomNode(rnd);

                        ClusterNode to = randomNode(rnd);

                        GridTestMessage msg = new GridTestMessage(from.id(), msgId.getAndIncrement(), 0);

                        spis.get(from.id()).sendMessage(to, msg);

                        ConcurrentLinkedDeque8<GridTestMessage> queue = msgs.get(to.id());

                        if (queue == null) {
                            ConcurrentLinkedDeque8<GridTestMessage> old = msgs.putIfAbsent(to.id(),
                                queue = new ConcurrentLinkedDeque8<>());

                            if (old != null)
                                queue = old;
                        }

                        queue.offer(msg);
                    }
                }
                catch (IgniteException e) {
                    log().error("Unable to send message.", e);

                    fail("Unable to send message: " + e.getMessage());
                }
            }
        }, getSpiCount() * 3, "message-sender");

        fut.get();

        info(">>> Sent all messages in " + (System.currentTimeMillis() - start) + " milliseconds");

        assertEquals("Invalid count of messages was sent", iterationCnt * getSpiCount() * 3, msgId.get());

        U.sleep(IDLE_CONN_TIMEOUT * 2);

        // Now validate all sent and received messages.
        for (Entry<UUID, ConcurrentLinkedDeque8<GridTestMessage>> e : msgs.entrySet()) {
            UUID to = e.getKey();

            ConcurrentLinkedDeque8<GridTestMessage> sent = e.getValue();

            MessageListener lsnr = lsnrs.get(to);

            ConcurrentLinkedDeque8<GridTestMessage> rcvd = lsnr.receivedMsgs();

            info(">>> Node " + to + " received " + lsnr.remoteMessageCount() + " remote messages of " +
                rcvd.sizex() + " total");

            for (int i = 0; i < 3 && sent.sizex() != rcvd.sizex(); i++) {
                info("Check failed for node [node=" + to + ", sent=" + sent.sizex() + ", rcvd=" + rcvd.sizex() + ']');

                U.sleep(2000);
            }

            assertEquals("Sent and received messages count mismatch.", sent.sizex(), rcvd.sizex());

            assertTrue("Listener did not receive some messages: " + lsnr, rcvd.containsAll(sent));
            assertTrue("Listener received extra messages: " + lsnr, sent.containsAll(rcvd));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlowSend() throws Exception {
        reject = true;

        final CyclicBarrier barrier = new CyclicBarrier(THREAD_CNT);

        final Random rnd = new Random();

        final ClusterNode from = randomNode(rnd);

        ClusterNode tmp;

        do {
            tmp = randomNode(rnd);
        }
        while (tmp.id().equals(from.id()));

        final ClusterNode to = tmp;

        final int iterationCnt = 1000;

        final AtomicInteger threadId = new AtomicInteger();

        final int interval = 50;

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    // Only first thread will print messages.
                    int id = threadId.getAndIncrement();

                    for (int i = 0; i < iterationCnt; i++) {
                        if (id == 0 && (i % 50) == 0)
                            info(">>> Running iteration " + i);

                        try {
                            for (ClusterNode node : nodes) {
                                Message msg =
                                    new GridTestMessage(from.id(), msgId.getAndIncrement(), 0);

                                spis.get(from.id()).sendMessage(node, msg);
                            }
                        }
                        catch (IgniteException e) {
                            log.warning(">>> Oops, unable to send message (safe to ignore).", e);
                        }

                        barrier.await();
                    }
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                catch (BrokenBarrierException e) {
                    info("Wait on barrier failed: " + e);

                    Thread.currentThread().interrupt();
                }
            }
        }, THREAD_CNT, "message-sender");

        final AtomicBoolean run = new AtomicBoolean(true);

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    while (run.get() && !Thread.currentThread().isInterrupted()) {
                        U.sleep(interval * 3 / 2);

                        ((TcpCommunicationSpi)spis.get(from.id())).onNodeLeft(to.id());
                    }
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }, 1);

        fut.get();

        run.set(false);

        fut2.get();

        // Wait when all messages are acknowledged to do not break next tests' logic.
        for (CommunicationSpi<Message> spi : spis.values()) {
            GridNioServer srv = U.field(spi, "nioSrvr");

            Collection<? extends GridNioSession> sessions = GridTestUtils.getFieldValue(srv, "sessions");

            for (GridNioSession ses : sessions) {
                final GridNioRecoveryDescriptor snd = ses.outRecoveryDescriptor();

                if (snd != null) {
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return snd.messagesRequests().isEmpty();
                        }
                    }, 10_000);

                    assertEquals("Unexpected messages: " + snd.messagesRequests(), 0,
                        snd.messagesRequests().size());
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPassThroughPerformance() throws Exception {
        reject = true;

        info(">>> Starting pass through performance test. <<<");

        assertEquals("Invalid listener count", getSpiCount(), lsnrs.size());

        final AtomicInteger cntr = new AtomicInteger();

        final int msgCnt = 5000;

        long start = System.currentTimeMillis();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    ClusterNode from = nodes.get(0);

                    ClusterNode to = nodes.get(1);

                    CommunicationSpi<Message> spi = spis.get(from.id());

                    while (cntr.getAndIncrement() < msgCnt) {
                        GridTestMessage msg = new GridTestMessage(from.id(), msgId.getAndIncrement(), 0);

                        msg.payload(new byte[10 * 1024]);

                        spi.sendMessage(to, msg);
                    }
                }
                catch (IgniteException e) {
                    fail("Unable to send message: " + e.getMessage());
                }
            }
        }, 5, "message-sender");

        fut.get();

        info(">>> Sent all messages in " + (System.currentTimeMillis() - start) + " milliseconds");

        assertEquals("Invalid count of messages was sent", msgCnt, msgId.get());
    }

    /**
     * Selects a random node from initialized nodes with given random.
     *
     * @param rnd Random to use.
     * @return Node.
     */
    private ClusterNode randomNode(Random rnd) {
        int idx = rnd.nextInt(nodes.size());

        return nodes.get(idx);
    }

    /**
     * @return Spi.
     */
    private CommunicationSpi<Message> newCommunicationSpi() {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        if (!useShmem)
            spi.setSharedMemoryPort(-1);

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(IDLE_CONN_TIMEOUT);

        return spi;
    }

    /**
     * @return Spi count.
     */
    private int getSpiCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        spis.clear();
        nodes.clear();
        spiRsrcs.clear();
        lsnrs.clear();

        Map<ClusterNode, GridSpiTestContext> ctxs = new HashMap<>();

        timeoutProcessor = new GridTimeoutProcessor(new GridTestKernalContext(log));

        timeoutProcessor.start();

        timeoutProcessor.onKernalStart();

        for (int i = 0; i < getSpiCount(); i++) {
            CommunicationSpi<Message> spi = newCommunicationSpi();

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "gridName", "grid-" + i);

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            node.order(i);

            GridSpiTestContext ctx = initSpiContext();

            ctx.timeoutProcessor(timeoutProcessor);

            ctx.setLocalNode(node);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            MessageListener lsnr = new MessageListener(rsrcs.getNodeId());

            spi.setListener(lsnr);

            lsnrs.put(rsrcs.getNodeId(), lsnr);

            info("Lsnrs: " + lsnrs);

            node.setAttributes(spi.getNodeAttributes());
            node.setAttribute(ATTR_MACS, F.concat(U.allLocalMACs(), ", "));

            nodes.add(node);

            spi.spiStart(getTestGridName() + (i + 1));

            spis.put(rsrcs.getNodeId(), spi);

            spi.onContextInitialized(ctx);

            ctxs.put(node, ctx);
        }

        // For each context set remote nodes.
        for (Entry<ClusterNode, GridSpiTestContext> e : ctxs.entrySet()) {
            for (ClusterNode n : nodes) {
                if (!n.equals(e.getKey()))
                    e.getValue().remoteNodes().add(n);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (MessageListener lsnr : lsnrs.values()) {
            lsnr.rcvdMsgs.clear();
            lsnr.rmtMsgCnt.set(0);
        }

        for (CommunicationSpi spi : spis.values()) {
            final ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(spi, "clients");

            assert GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    for (GridCommunicationClient[] clients0 : clients.values()) {
                        for (GridCommunicationClient client : clients0) {
                            if (client != null)
                                return false;
                        }
                    }

                    return true;
                }
            }, getTestTimeout()) : "Clients: " + clients;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (timeoutProcessor != null) {
            timeoutProcessor.onKernalStop(true);

            timeoutProcessor.stop(true);

            timeoutProcessor = null;
        }

        for (CommunicationSpi<Message> spi : spis.values()) {
            spi.onContextDestroyed();

            spi.setListener(null);

            spi.spiStop();
        }

        for (IgniteTestResources rsrcs : spiRsrcs)
            rsrcs.stopThreads();

        lsnrs.clear();
        spiRsrcs.clear();
        spis.clear();
        nodes.clear();
    }
}
