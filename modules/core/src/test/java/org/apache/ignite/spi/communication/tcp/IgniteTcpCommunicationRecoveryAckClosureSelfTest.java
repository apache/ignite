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

import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

/**
 *
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class IgniteTcpCommunicationRecoveryAckClosureSelfTest<T extends CommunicationSpi>
    extends GridSpiAbstractTest<T> {
    /** */
    private static final Collection<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /** */
    protected static final List<TcpCommunicationSpi> spis = new ArrayList<>();

    /** */
    protected static final List<ClusterNode> nodes = new ArrayList<>();

    /** */
    private static final int SPI_CNT = 2;

    /**
     *
     */
    static {
        GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new GridTestMessage();
            }
        });
    }

    /**
     * Disable SPI auto-start.
     */
    public IgniteTcpCommunicationRecoveryAckClosureSelfTest() {
        super(false);
    }

    /** */
    private class TestListener implements CommunicationListener<Message> {
        /** */
        private ConcurrentHashSet<Long> msgIds = new ConcurrentHashSet<>();

        /** */
        private AtomicInteger rcvCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            info("Test listener received message: " + msg);

            assertTrue("Unexpected message: " + msg, msg instanceof GridTestMessage);

            GridTestMessage msg0 = (GridTestMessage)msg;

            assertTrue("Duplicated message received: " + msg0, msgIds.add(msg0.getMsgId()));

            rcvCnt.incrementAndGet();

            msgC.run();
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAckOnIdle() throws Exception {
        checkAck(10, 2000, 9);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAckOnCount() throws Exception {
        checkAck(10, 60_000, 10);
    }

    /**
     * @param ackCnt Recovery acknowledgement count.
     * @param idleTimeout Idle connection timeout.
     * @param msgPerIter Messages per iteration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkAck(int ackCnt, int idleTimeout, int msgPerIter) throws Exception {
        createSpis(ackCnt, idleTimeout, TcpCommunicationSpi.DFLT_MSG_QUEUE_LIMIT);

        try {
            TcpCommunicationSpi spi0 = spis.get(0);
            TcpCommunicationSpi spi1 = spis.get(1);

            ClusterNode node0 = nodes.get(0);
            ClusterNode node1 = nodes.get(1);

            int msgId = 0;

            int expMsgs = 0;

            long totAcked = 0;

            for (int i = 0; i < 5; i++) {
                info("Iteration: " + i);

                final AtomicInteger ackMsgs = new AtomicInteger(0);

                IgniteInClosure<IgniteException> ackC = new CI1<IgniteException>() {
                    @Override public void apply(IgniteException o) {
                        assert o == null;

                        ackMsgs.incrementAndGet();
                    }
                };

                for (int j = 0; j < msgPerIter; j++) {
                    spi0.sendMessage(node1, new GridTestMessage(node0.id(), ++msgId, 0), ackC);

                    spi1.sendMessage(node0, new GridTestMessage(node1.id(), ++msgId, 0), ackC);
                }

                expMsgs += msgPerIter;

                final long totAcked0 = totAcked;

                for (TcpCommunicationSpi spi : spis) {
                    GridNioServer srv = U.field(spi, "nioSrvr");

                    Collection<? extends GridNioSession> sessions = GridTestUtils.getFieldValue(srv, "sessions");

                    assertFalse(sessions.isEmpty());

                    boolean found = false;

                    for (GridNioSession ses : sessions) {
                        final GridNioRecoveryDescriptor recoveryDesc = ses.outRecoveryDescriptor();

                        if (recoveryDesc != null) {
                            found = true;

                            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                                @Override public boolean apply() {
                                    long acked = GridTestUtils.getFieldValue(recoveryDesc, "acked");

                                    return acked > totAcked0;
                                }
                            }, 5000);

                            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                                @Override public boolean apply() {
                                    return recoveryDesc.messagesRequests().isEmpty();
                                }
                            }, 10_000);

                            assertEquals("Unexpected messages: " + recoveryDesc.messagesRequests(), 0,
                                recoveryDesc.messagesRequests().size());

                            break;
                        }
                    }

                    assertTrue(found);
                }

                final int expMsgs0 = expMsgs;

                for (TcpCommunicationSpi spi : spis) {
                    final TestListener lsnr = (TestListener)spi.getListener();

                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return lsnr.rcvCnt.get() >= expMsgs0;
                        }
                    }, 5000);

                    assertEquals(expMsgs, lsnr.rcvCnt.get());
                }

                assertEquals(msgPerIter * 2, ackMsgs.get());

                totAcked += msgPerIter;
            }
        }
        finally {
            stopSpis();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueueOverflow() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-172");

        for (int i = 0; i < 3; i++) {
            try {
                startSpis(5, 60_000, 10);

                checkOverflow();

                break;
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(BindException.class)) {
                    if (i < 2) {
                        info("Got exception caused by BindException, will retry after delay: " + e);

                        stopSpis();

                        U.sleep(10_000);
                    }
                    else
                        throw e;
                }
                else
                    throw e;
            }
            finally {
                stopSpis();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkOverflow() throws Exception {
        TcpCommunicationSpi spi0 = spis.get(0);
        TcpCommunicationSpi spi1 = spis.get(1);

        ClusterNode node0 = nodes.get(0);
        ClusterNode node1 = nodes.get(1);

        final GridNioServer srv1 = U.field(spi1, "nioSrvr");

        final AtomicInteger ackMsgs = new AtomicInteger(0);

        IgniteInClosure<IgniteException> ackC = new CI1<IgniteException>() {
            @Override public void apply(IgniteException o) {
                assert o == null;

                ackMsgs.incrementAndGet();
            }
        };

        int msgId = 0;

        // Send message to establish connection.
        spi0.sendMessage(node1, new GridTestMessage(node0.id(), ++msgId, 0), ackC);

        // Prevent node1 from send
        GridTestUtils.setFieldValue(srv1, "skipWrite", true);

        final GridNioSession ses0 = communicationSession(spi0);

        int sentMsgs = 1;

        for (int i = 0; i < 150; i++) {
            try {
                spi0.sendMessage(node1, new GridTestMessage(node0.id(), ++msgId, 0), ackC);

                sentMsgs++;
            }
            catch (IgniteSpiException e) {
                log.info("Send error [err=" + e + ", sentMsgs=" + sentMsgs + ']');

                break;
            }
        }

        // Wait when session is closed because of queue overflow.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ses0.closeTime() != 0;
            }
        }, 5000);

        assertTrue("Failed to wait for session close", ses0.closeTime() != 0);

        GridTestUtils.setFieldValue(srv1, "skipWrite", false);

        for (int i = 0; i < 100; i++)
            spi0.sendMessage(node1, new GridTestMessage(node0.id(), ++msgId, 0), ackC);

        final int expMsgs = sentMsgs + 100;

        final TestListener lsnr = (TestListener)spi1.getListener();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return lsnr.rcvCnt.get() >= expMsgs;
            }
        }, 5000);

        assertEquals(expMsgs, lsnr.rcvCnt.get());

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expMsgs == ackMsgs.get();
            }
        }, 5000);

        assertEquals(expMsgs, ackMsgs.get());
    }

    /**
     * @param spi SPI.
     * @return Session.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private GridNioSession communicationSession(TcpCommunicationSpi spi) throws Exception {
        final GridNioServer srv = U.field(spi, "nioSrvr");

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Collection<? extends GridNioSession> sessions = GridTestUtils.getFieldValue(srv, "sessions");

                return !sessions.isEmpty();
            }
        }, 5000);

        Collection<? extends GridNioSession> sessions = GridTestUtils.getFieldValue(srv, "sessions");

        assertEquals(1, sessions.size());

        return sessions.iterator().next();
    }

    /**
     * @param ackCnt Recovery acknowledgement count.
     * @param idleTimeout Idle connection timeout.
     * @param queueLimit Message queue limit.
     * @return SPI instance.
     */
    protected TcpCommunicationSpi getSpi(int ackCnt, int idleTimeout, int queueLimit) {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(idleTimeout);
        spi.setTcpNoDelay(true);
        spi.setAckSendThreshold(ackCnt);
        spi.setMessageQueueLimit(queueLimit);
        spi.setSharedMemoryPort(-1);
        spi.setConnectionsPerNode(1);

        return spi;
    }

    /**
     * @param ackCnt Recovery acknowledgement count.
     * @param idleTimeout Idle connection timeout.
     * @param queueLimit Message queue limit.
     * @throws Exception If failed.
     */
    private void startSpis(int ackCnt, int idleTimeout, int queueLimit) throws Exception {
        spis.clear();
        nodes.clear();
        spiRsrcs.clear();

        Map<ClusterNode, GridSpiTestContext> ctxs = new HashMap<>();

        for (int i = 0; i < SPI_CNT; i++) {
            TcpCommunicationSpi spi = getSpi(ackCnt, idleTimeout, queueLimit);

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "gridName", "grid-" + i);

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            GridSpiTestContext ctx = initSpiContext();

            ctx.setLocalNode(node);

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            spi.setListener(new TestListener());

            node.setAttributes(spi.getNodeAttributes());

            nodes.add(node);

            spi.spiStart(getTestGridName() + (i + 1));

            spis.add(spi);

            spi.onContextInitialized(ctx);

            ctxs.put(node, ctx);
        }

        // For each context set remote nodes.
        for (Map.Entry<ClusterNode, GridSpiTestContext> e : ctxs.entrySet()) {
            for (ClusterNode n : nodes) {
                if (!n.equals(e.getKey()))
                    e.getValue().remoteNodes().add(n);
            }
        }
    }

    /**
     * @param ackCnt Recovery acknowledgement count.
     * @param idleTimeout Idle connection timeout.
     * @param queueLimit Message queue limit.
     * @throws Exception If failed.
     */
    private void createSpis(int ackCnt, int idleTimeout, int queueLimit) throws Exception {
        for (int i = 0; i < 3; i++) {
            try {
                startSpis(ackCnt, idleTimeout, queueLimit);

                break;
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(BindException.class)) {
                    if (i < 2) {
                        info("Failed to start SPIs because of BindException, will retry after delay.");

                        stopSpis();

                        U.sleep(10_000);
                    }
                    else
                        throw e;
                }
                else
                    throw e;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void stopSpis() throws Exception {
        for (CommunicationSpi<Message> spi : spis) {
            spi.onContextDestroyed();

            spi.setListener(null);

            spi.spiStop();
        }

        for (IgniteTestResources rsrcs : spiRsrcs)
            rsrcs.stopThreads();

        spis.clear();
        nodes.clear();
        spiRsrcs.clear();
    }
}