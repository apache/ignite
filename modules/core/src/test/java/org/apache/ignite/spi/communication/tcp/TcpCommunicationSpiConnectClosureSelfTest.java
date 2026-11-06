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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.communication.tcp.internal.ConnectFuture;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.internal.GridNioServerWrapper;
import org.apache.ignite.spi.communication.tcp.internal.HandshakeTimeoutObject;
import org.apache.ignite.spi.communication.tcp.internal.NodeUnreachableException;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.spi.communication.GridTestMessage.GRID_TEST_MESSAGE_FACTORY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the connection establishment of the TCP communication SPI when two nodes re-establish their
 * communication connection simultaneously: the node with the lower order wins the race (the established
 * connection is outgoing on the lower-ordered node and incoming on the higher-ordered node, which is the
 * scenario predicate checked by the test), the messages of both nodes are delivered over the
 * re-established connection, and the client pools and recovery descriptors stay in a healthy state
 * (no leaked connect futures, no stuck reserved descriptors).
 *
 * <p>A second test stalls the NIO session registration of a new outgoing connection through the channel
 * registrar and proves that the deadline of the handshake and the NIO session registration, armed
 * after the socket connect, closes the channel while the stalled registration is still pending: the
 * stalled registration fails, the failed attempt is retried with an increased timeout, and the
 * message is delivered over the re-established connection.</p>
 *
 * <p>A third test slows the socket connect of a new outgoing connection and stalls its NIO session
 * registration and proves that the connect does not consume the deadline of the handshake and the
 * registration that follow it: the deadline, armed after the connect with the handshake window, still
 * covers the stalled registration, so the message is delivered without a deadline-failed retry.</p>
 *
 * <p>A fourth test verifies the deadline object itself: a cancelled deadline is never reported as
 * fired and never closes the connection, while a fired deadline is reported as such, so a caller
 * whose cancel lost the race to the timeout task knows that the deadline actually elapsed and the
 * connection was closed, instead of only observing that the task could no longer be cancelled.</p>
 *
 * <p>A fifth test fails three consecutive connection attempts by stalling their NIO session
 * registration until the establishment deadline closes the channel, and verifies that the
 * establishment window of each attempt increases with the exponential connection backoff, so the
 * handshake and registration timeout progression across the retries is preserved.</p>
 *
 * <p>The remaining tests bound the client reservation of the connection pool: a waiter on a connect
 * future that never completes must give up after the total reserve budget instead of waiting
 * indefinitely, an exhausted reserve budget must not prevent returning a client that a concurrent
 * thread has established in the meantime, a disabled system worker blocked timeout must not abort
 * the waiter immediately, a stopped SPI must complete the pending connect futures so that all
 * waiters terminate, the wait for a peer-initiated (inverse) connection must be bounded by its own
 * window (the connect timeout when the failure detection timeout is disabled) instead of the
 * reservation budget that the failed direct attempts have already consumed, and a waiter on an
 * in-progress connect future must still be released by its own reserve budget while the producer of
 * that connection is in the inverse-connection phase.</p>
 *
 * <p>The fault-injection variants of the pending connect closure path (a failed recovery handshake write
 * and a held recovery handshake completion) are intentionally not covered: they require a deterministic
 * recorder of NIO connection and write events embedded in the production classes, which was considered
 * disproportionate for the scenarios. The invariants those tests asserted are pre-existing behavior, not
 * part of the changes under test here: a failed recovery handshake write leaves the recovery descriptor
 * cleanly released, a successfully released held completion self-heals the communication through client
 * adoption on the existing session, and a failed held completion tears down the half-open connection so
 * that the communication is re-established from scratch.</p>
 *
 * <p>The tests run without SSL. {@link TcpCommunicationSpiConnectClosureSslSelfTest} runs the same
 * scenarios with SSL, where the outgoing TLS handshake happens outside of the regular NIO write path, so
 * the connection establishment must be verified under these conditions as well.</p>
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class TcpCommunicationSpiConnectClosureSelfTest<T extends CommunicationSpi<Message>>
    extends GridSpiAbstractTest<T> {
    /**
     * Number of SPIs.
     */
    private static final int SPI_CNT = 2;

    /**
     * Maximum number of reconnect attempts to observe a simultaneous connection establishment.
     */
    private static final int MAX_ITERS = 50;

    /**
     * Wait timeout in seconds.
     */
    private static final long AWAIT_TIMEOUT = 10;

    /**
     * SPIs.
     */
    private final List<TcpCommunicationSpi> spis = new ArrayList<>();

    /**
     * Nodes.
     */
    private final List<ClusterNode> nodes = new ArrayList<>();

    /**
     * SPI test resources.
     */
    private final List<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /**
     * Communication listeners of the SPIs, indexed by SPI.
     */
    private final List<TestListener> listeners = new ArrayList<>();

    /**
     * Timeout processor.
     */
    private GridTimeoutProcessor timeoutProc;

    /**
     * Use SSL.
     */
    protected boolean useSsl;

    /**
     * Disable SPI auto-start.
     */
    public TcpCommunicationSpiConnectClosureSelfTest() {
        super(false);
    }

    /**
     * Closes an established connection on both ends and makes both nodes start a new outgoing connection
     * simultaneously: the established connection must be the one initiated by the lower-ordered node
     * (outgoing on node 0, incoming on node 1, which is the scenario predicate for a simultaneous
     * reconnect), both messages must be delivered over the re-established connection, and the client
     * pools and recovery descriptors must be in a healthy state.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConnectClosureOnSimultaneousReconnect() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);
            TcpCommunicationSpi spi1 = spis.get(1);

            ClusterNode node0 = nodes.get(0);
            ClusterNode node1 = nodes.get(1);

            assertEquals(1, node0.order());
            assertEquals(2, node1.order());

            // Establish the initial communication connection.
            CountDownLatch ack = new CountDownLatch(1);

            AtomicReference<Throwable> initErr = new AtomicReference<>();

            spi0.sendMessage(node1, new GridTestMessage(node0.id(), 1, 0), err -> {
                try {
                    initErr.compareAndSet(null, err);
                }
                finally {
                    ack.countDown();
                }
            });

            assertTrue("Failed to establish initial connection", ack.await(AWAIT_TIMEOUT, TimeUnit.SECONDS));

            assertNull("Unexpected error during initial connection", initErr.get());

            GridNioServer<?> srv0 = ((GridNioServerWrapper)U.field(spi0, "nioSrvWrapper")).nio();
            GridNioServer<?> srv1 = ((GridNioServerWrapper)U.field(spi1, "nioSrvWrapper")).nio();

            for (int iter = 0; iter < MAX_ITERS; iter++) {
                info("Simultaneous reconnect attempt [iter=" + iter + ']');

                // Close the established connection on both ends.
                closeAllSessions(srv0);

                closeAllSessions(srv1);

                assertTrue("Failed to wait for connection pools to become empty [iter=" + iter + ']',
                    waitForPoolsEmpty(spi0, node1, spi1, node0));

                // Both nodes start a new outgoing connection simultaneously.
                CyclicBarrier barrier = new CyclicBarrier(2);

                AtomicReference<Throwable> sendErr = new AtomicReference<>();

                Thread th0 = startSendThread(barrier, spi0, node1, node0, 100 + iter, sendErr, null);
                Thread th1 = startSendThread(barrier, spi1, node0, node1, 200 + iter, sendErr, null);

                // Delivery is verified at the receiving listeners: the send callback depends on the
                // recovery ack counters, which the reconnection desynchronizes, so the callback alone
                // does not prove delivery.
                final int msgIter = iter;

                assertTrue("Failed to deliver messages over the re-established connection [iter=" + iter
                        + "]: received by node1=" + listeners.get(1).receivedMsgIds()
                        + " by node0=" + listeners.get(0).receivedMsgIds(),
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return listeners.get(1).received(100 + msgIter)
                                && listeners.get(0).received(200 + msgIter);
                        }
                    }, AWAIT_TIMEOUT * 1000L));

                th0.join(AWAIT_TIMEOUT * 1000L);
                th1.join(AWAIT_TIMEOUT * 1000L);

                assertFalse("First sender thread did not terminate [iter=" + iter + ']', th0.isAlive());

                assertFalse("Second sender thread did not terminate [iter=" + iter + ']', th1.isAlive());

                assertNull("Unexpected send error [iter=" + iter + ']', sendErr.get());

                // The established connection must be the one initiated by node 0 (lower order): outgoing
                // on node 0, incoming on node 1. This direction is the scenario predicate: it requires
                // both nodes to have reconnected simultaneously, so node 1 (higher order) rejected the
                // simultaneous incoming connection.
                boolean direction = srv0.sessions().size() == 1 && srv1.sessions().size() == 1
                    && !srv0.sessions().iterator().next().accepted()
                    && srv1.sessions().iterator().next().accepted();

                if (!direction) {
                    info("Simultaneous reconnect in the expected direction not observed, retrying "
                        + "[iter=" + iter + ']');

                    continue;
                }

                // No leaked connect futures in the client pools at rest.
                ConnectionClientPool pool0 = U.field(spi0, "clientPool");
                ConnectionClientPool pool1 = U.field(spi1, "clientPool");

                ConcurrentMap<?, ?> futs0 = U.field(pool0, "clientFuts");
                ConcurrentMap<?, ?> futs1 = U.field(pool1, "clientFuts");

                assertTrue("Connect futures leaked in the client pool of node 0 [iter=" + iter + "]: " + futs0,
                    futs0.isEmpty());

                assertTrue("Connect futures leaked in the client pool of node 1 [iter=" + iter + "]: " + futs1,
                    futs1.isEmpty());

                // Descriptor hygiene: a recovery descriptor may be reserved without being connected only
                // while it is held by the live session of an active outgoing connection. A
                // reserved-but-not-connected descriptor that is not held by the live session is stuck:
                // it blocks every future connection to the peer until the reservation timeout. The
                // handshake of the re-established connection must additionally have been registered on
                // at least one side.
                GridNioServerWrapper wrap0 = U.field(spi0, "nioSrvWrapper");
                GridNioServerWrapper wrap1 = U.field(spi1, "nioSrvWrapper");

                ConcurrentMap<?, GridNioRecoveryDescriptor> descs0 = U.field(wrap0, "recoveryDescs");
                ConcurrentMap<?, GridNioRecoveryDescriptor> descs1 = U.field(wrap1, "recoveryDescs");

                GridNioSession live0 = srv0.sessions().iterator().next();
                GridNioSession live1 = srv1.sessions().iterator().next();

                boolean found0 = false;
                boolean found1 = false;

                for (GridNioRecoveryDescriptor desc : descs0.values()) {
                    if (desc.node().equals(node1)) {
                        found0 = true;

                        if (desc.reserved() && !desc.connected()
                            && desc != live0.outRecoveryDescriptor() && desc != live0.inRecoveryDescriptor())
                            fail("Recovery descriptor of node 1 for node 2 is stuck in reserved state "
                                + "after reconnection (not held by the live session): " + desc);
                    }
                }

                for (GridNioRecoveryDescriptor desc : descs1.values()) {
                    if (desc.node().equals(node0)) {
                        found1 = true;

                        if (desc.reserved() && !desc.connected()
                            && desc != live1.outRecoveryDescriptor() && desc != live1.inRecoveryDescriptor())
                            fail("Recovery descriptor of node 2 for node 1 is stuck in reserved state "
                                + "after reconnection (not held by the live session): " + desc);
                    }
                }

                assertTrue("No recovery descriptor found for node 2 on node 1", found0);

                assertTrue("No recovery descriptor found for node 1 on node 2", found1);

                // The handshake of the re-established connection must be registered: check it on the
                // recovery descriptors actually held by the live sessions, not on any other descriptor
                // for the peer.
                GridNioRecoveryDescriptor liveOut0 = live0.outRecoveryDescriptor();
                GridNioRecoveryDescriptor liveIn0 = live0.inRecoveryDescriptor();
                GridNioRecoveryDescriptor liveOut1 = live1.outRecoveryDescriptor();
                GridNioRecoveryDescriptor liveIn1 = live1.inRecoveryDescriptor();

                boolean liveConnected0 = (liveOut0 != null && liveOut0.connected())
                    || (liveIn0 != null && liveIn0.connected());

                boolean liveConnected1 = (liveOut1 != null && liveOut1.connected())
                    || (liveIn1 != null && liveIn1.connected());

                assertTrue("Recovery handshake of the re-established connection was not registered on "
                    + "either side", liveConnected0 || liveConnected1);

                return;
            }

            fail("Failed to observe a simultaneous reconnect within " + MAX_ITERS + " iterations");
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Stalls the NIO session registration of a new outgoing connection via the channel registrar and
     * checks that the single connection establishment deadline, which is scheduled before the TCP
     * handshake, elapses while the registration is still pending: the deadline closes the channel, so
     * the stalled registration fails, the failed attempt is retried with an increased timeout, and the
     * message is delivered over the re-established connection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionDeadlineCoversSessionRegistration() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);
            TcpCommunicationSpi spi1 = spis.get(1);

            ClusterNode node0 = nodes.get(0);
            ClusterNode node1 = nodes.get(1);

            // Establish the initial communication connection.
            CountDownLatch ack = new CountDownLatch(1);

            AtomicReference<Throwable> initErr = new AtomicReference<>();

            spi0.sendMessage(node1, new GridTestMessage(node0.id(), 1, 0), err -> {
                try {
                    initErr.compareAndSet(null, err);
                }
                finally {
                    ack.countDown();
                }
            });

            assertTrue("Failed to establish initial connection", ack.await(AWAIT_TIMEOUT, TimeUnit.SECONDS));

            assertNull("Unexpected error during initial connection", initErr.get());

            // Shorten the connection establishment deadline for this test so that the stalled
            // registration is bounded by a short, known budget (the explicit connect timeout disables
            // the failure detection timeout, so the first attempt's connect window is the connect
            // timeout, and the deadline of the handshake and registration that follow it is the same
            // value re-verified against the strategy's total timeout).
            int deadlineMs = 5000;

            spi0.setConnectTimeout(deadlineMs);

            spi1.setConnectTimeout(deadlineMs);

            GridNioServer<?> srv0 = ((GridNioServerWrapper)U.field(spi0, "nioSrvWrapper")).nio();
            GridNioServer<?> srv1 = ((GridNioServerWrapper)U.field(spi1, "nioSrvWrapper")).nio();

            // Close the established connection.
            closeAllSessions(srv0);

            closeAllSessions(srv1);

            assertTrue("Failed to wait for connection pools to become empty",
                waitForPoolsEmpty(spi0, node1, spi1, node0));

            AtomicLong attemptStartNs = new AtomicLong();
            AtomicLong entryOffsetMs = new AtomicLong(-1);
            AtomicLong closeOffsetMs = new AtomicLong(-1);
            AtomicInteger outgoingRegistrations = new AtomicInteger();

            // The registrar holds the first outgoing (non-accepted) registration of node 0: the NIO
            // worker blocks in the registration until the connection establishment deadline closes
            // the channel. The hard limit, longer than the deadline, keeps the worker bounded if the
            // deadline fails to fire. The accepted session and the retried outgoing registration are
            // not held.
            srv0.setChannelRegistrar((channel, selector, ops, attachment) -> {
                GridNioSession ses = (GridNioSession)attachment;

                if (!ses.accepted() && outgoingRegistrations.getAndIncrement() == 0) {
                    entryOffsetMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - attemptStartNs.get()));

                    long heldSinceNs = System.nanoTime();

                    try {
                        while (channel.isOpen()
                            && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - heldSinceNs) < deadlineMs + 3000)
                            Thread.sleep(50);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    if (!channel.isOpen())
                        closeOffsetMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - attemptStartNs.get()));
                }

                return channel.register(selector, ops, attachment);
            });

            try {
                // The explicit connect timeout disables failure detection, so the first connection
                // attempt is bounded by the configured 5000 ms connect timeout. The attempt starts
                // right after the send is triggered: the anchor is captured in the send thread,
                // right before the sendMessage call.
                AtomicReference<Throwable> sendErr = new AtomicReference<>();

                // sendMessage blocks the calling thread until the connection (including the NIO session
                // registration) is established, so it must run on a separate thread while the test
                // thread observes the deadline.
                Thread sendTh = startSendThread(new CyclicBarrier(1), spi0, node1, node0, 900, sendErr,
                    attemptStartNs);

                // The registration of the new outgoing connection must have been reached.
                assertTrue("Failed to reach the stalled session registration",
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return entryOffsetMs.get() >= 0;
                        }
                    }, AWAIT_TIMEOUT * 1000L));

                // The registration must have started before the deadline could fire (the deadline is
                // armed right after the socket connect, which happens right after the send is
                // triggered): the deadline covers the stalled registration.
                assertTrue("The stalled registration did not start before the deadline could fire "
                        + "[entryOffsetMs=" + entryOffsetMs.get() + ", deadlineMs=" + deadlineMs + ']',
                    entryOffsetMs.get() >= 0 && entryOffsetMs.get() < deadlineMs - 500);

                // The failed first attempt must be retried, and the message must be delivered over
                // the re-established connection.
                sendTh.join(AWAIT_TIMEOUT * 1000L);

                assertFalse("Send thread did not finish after the stalled registration was released",
                    sendTh.isAlive());

                assertNull("Unexpected send error", sendErr.get());

                // The deadline must have closed the channel while the registration was still
                // pending: the offset is measured from the send trigger (the attempt starts right
                // after the trigger, and the deadline is armed right after the fast socket connect),
                // at a stable value around the connect timeout, not from the registration, which was
                // stalled.
                assertTrue("The connection establishment deadline did not close the stalled channel "
                        + "while the registration was pending, or it fired at an unexpected offset from "
                        + "the send trigger [closeOffsetMs=" + closeOffsetMs.get() + ", deadlineMs="
                        + deadlineMs + "]",
                    closeOffsetMs.get() >= deadlineMs - 1000 && closeOffsetMs.get() <= deadlineMs + 2000);

                assertTrue("Message was not delivered over the re-established connection",
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return listeners.get(1).received(900);
                        }
                    }, AWAIT_TIMEOUT * 1000L));

                assertTrue("Expected the re-established outgoing session on node 0",
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return srv0.sessions().size() == 1;
                        }
                    }, AWAIT_TIMEOUT * 1000L));

                assertFalse("Expected an outgoing session on node 0",
                    srv0.sessions().iterator().next().accepted());

                // The failed first attempt must not have leaked pool state: the retry established a
                // healthy client and no connect future for the stalled attempt is pending anymore.
                ConnectionClientPool pool0 = U.field(spi0, "clientPool");

                GridCommunicationClient[] clients = pool0.clientFor(node1.id());

                assertNotNull("The re-established connection did not leave a client in the pool", clients);

                assertNotNull("The re-established connection did not leave a client in the pool", clients[0]);

                ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
                    U.field(pool0, "clientFuts");

                for (GridFutureAdapter<GridCommunicationClient> fut : clientFuts.values())
                    assertFalse("A connect future of the stalled attempt is still pending: " + fut, !fut.isDone());
            }
            finally {
                srv0.setChannelRegistrar(null);
            }
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Slows the socket connect of a new outgoing connection and stalls its NIO session registration
     * and checks that the connect does not consume the deadline of the handshake and the registration
     * that follow it: the deadline is armed after the socket connect (which is bounded by its own
     * connect timeout) with the handshake window, so the stalled registration, which exceeds the time
     * left of a deadline covering the whole establishment from its start, still fits into the
     * deadline, and the message is delivered without a deadline-failed retry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionDeadlineAfterSlowConnect() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);
            TcpCommunicationSpi spi1 = spis.get(1);

            ClusterNode node0 = nodes.get(0);
            ClusterNode node1 = nodes.get(1);

            // Establish the initial communication connection.
            CountDownLatch ack = new CountDownLatch(1);

            AtomicReference<Throwable> initErr = new AtomicReference<>();

            spi0.sendMessage(node1, new GridTestMessage(node0.id(), 1, 0), err -> {
                try {
                    initErr.compareAndSet(null, err);
                }
                finally {
                    ack.countDown();
                }
            });

            assertTrue("Failed to establish initial connection", ack.await(AWAIT_TIMEOUT, TimeUnit.SECONDS));

            assertNull("Unexpected error during initial connection", initErr.get());

            // The connect timeout of the first attempt of a new connection (the explicit connect
            // timeout disables the failure detection timeout), and with it the deadline of the
            // handshake and registration that follow it: the slow connect consumes most of it, so a
            // deadline covering the whole establishment from its start would expire before the
            // stalled registration completes.
            int deadlineMs = 5000;

            spi0.setConnectTimeout(deadlineMs);

            spi1.setConnectTimeout(deadlineMs);

            // The slow connect consumes most of the connect window.
            int slowConnectMs = 4000;

            // The stalled registration exceeds the budget a whole-establishment deadline would have
            // left after the slow connect, but fits into the deadline armed after the connect.
            int stalledRegistrationMs = 1500;

            GridNioServer<?> srv0 = ((GridNioServerWrapper)U.field(spi0, "nioSrvWrapper")).nio();
            GridNioServer<?> srv1 = ((GridNioServerWrapper)U.field(spi1, "nioSrvWrapper")).nio();

            GridNioServerWrapper wrapper0 = (GridNioServerWrapper)U.field(spi0, "nioSrvWrapper");

            // Close the established connection.
            closeAllSessions(srv0);

            closeAllSessions(srv1);

            assertTrue("Failed to wait for connection pools to become empty",
                waitForPoolsEmpty(spi0, node1, spi1, node0));

            AtomicLong attemptStartNs = new AtomicLong();

            AtomicInteger slowConnects = new AtomicInteger();

            AtomicInteger outgoingRegistrations = new AtomicInteger();

            AtomicLong releaseOffsetMs = new AtomicLong(-1);

            AtomicLong closedOffsetMs = new AtomicLong(-1);

            // Slow the first socket connect of the reconnection: the forwarding channel wrapper delays
            // the connect through its socket and delegates every other channel operation to the real
            // channel, so the real channel keeps its own state machine and the delayed connect
            // behaves like a slow network connection.
            wrapper0.socketChannelFactory(() ->
                new SlowConnectChannel(SocketChannel.open(), slowConnects, slowConnectMs));

            // The registrar stalls the first outgoing (non-accepted) registration of node 0: if a
            // deadline covering the whole establishment from its start were armed, it would close the
            // channel while the stall is still pending (the offset is recorded in that case), while
            // the deadline armed after the connect lets the stall finish.
            srv0.setChannelRegistrar((channel, selector, ops, attachment) -> {
                GridNioSession ses = (GridNioSession)attachment;

                if (!ses.accepted() && outgoingRegistrations.getAndIncrement() == 0) {
                    long heldSinceNs = System.nanoTime();

                    try {
                        while (channel.isOpen()
                            && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - heldSinceNs) < stalledRegistrationMs)
                            Thread.sleep(50);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    if (channel.isOpen())
                        releaseOffsetMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - heldSinceNs));
                    else
                        closedOffsetMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - attemptStartNs.get()));
                }

                // The JDK selector only accepts its own internal channel implementation, so register
                // the real channel the wrapper forwards to: the NIO worker performs all I/O through
                // the channel of the registered selection key.
                SocketChannel target = (channel instanceof SlowConnectChannel)
                    ? ((SlowConnectChannel)channel).real
                    : channel;

                return target.register(selector, ops, attachment);
            });

            try {
                AtomicReference<Throwable> sendErr = new AtomicReference<>();

                // sendMessage blocks the calling thread until the connection (including the NIO session
                // registration) is established, so it must run on a separate thread while the test
                // thread observes the deadline.
                Thread sendTh = startSendThread(new CyclicBarrier(1), spi0, node1, node0, 900, sendErr,
                    attemptStartNs);

                // The stalled registration must have been released while the channel was still open.
                assertTrue("The stalled registration was not released while the channel was open "
                        + "[closedOffsetMs=" + closedOffsetMs.get() + ']',
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return releaseOffsetMs.get() >= 0;
                        }
                    }, AWAIT_TIMEOUT * 1000L));

                sendTh.join(AWAIT_TIMEOUT * 1000L);

                assertFalse("Send thread did not finish after the stalled registration was released",
                    sendTh.isAlive());

                assertNull("Unexpected send error", sendErr.get());

                assertEquals("The slow connect was not consumed exactly once: the first attempt must have "
                        + "succeeded within the deadline armed after the connect, without a retry",
                    1, slowConnects.get());

                assertTrue("The stalled registration was closed by a deadline before it was released: the "
                        + "connect must not consume the deadline of the handshake and registration that "
                        + "follow it [closedOffsetMs=" + closedOffsetMs.get() + ']',
                    closedOffsetMs.get() < 0);

                assertTrue("Message was not delivered over the re-established connection",
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return listeners.get(1).received(900);
                        }
                    }, AWAIT_TIMEOUT * 1000L));
            }
            finally {
                srv0.setChannelRegistrar(null);

                wrapper0.socketChannelFactory(SocketChannel::open);
            }
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Verifies that the handshake deadline object reports the deadline outcome unambiguously: a
     * cancelled deadline must not be reported as fired and must not close the connection, and a fired
     * deadline must be reported as such, so a caller whose cancel lost the race to the timeout task
     * knows that the deadline actually elapsed and the connection was closed, instead of only
     * observing that the task could no longer be cancelled.
     */
    @Test
    public void testHandshakeDeadlineDistinguishesFiredDeadlineFromCancelled() throws Exception {
        // Cancelled before the deadline fired.
        SocketChannel cancelledCh = SocketChannel.open();

        try {
            HandshakeTimeoutObject cancelled = new HandshakeTimeoutObject(cancelledCh);

            assertTrue("The deadline must be cancellable before it fires", cancelled.cancel());
            assertFalse("A cancelled deadline must not be reported as fired", cancelled.timedOut());

            cancelled.run();

            assertTrue("The timeout task must not close a connection whose deadline was cancelled",
                cancelledCh.isOpen());
            assertFalse("A timeout task that lost the race to cancel must not report a fired deadline",
                cancelled.timedOut());
        }
        finally {
            cancelledCh.close();
        }

        // Fired deadline.
        SocketChannel firedCh = SocketChannel.open();

        try {
            HandshakeTimeoutObject fired = new HandshakeTimeoutObject(firedCh);

            fired.run();

            assertTrue("A fired deadline must be reported as fired", fired.timedOut());
            assertFalse("A fired deadline must no longer be cancellable", fired.cancel());
            assertTrue("The timeout task must close the connection once the deadline fired", !firedCh.isOpen());
        }
        finally {
            U.closeQuiet(firedCh);
        }
    }

    /**
     * Fails three consecutive connection attempts by stalling their NIO session registration until
     * the establishment deadline closes the channel, and verifies that the establishment window of
     * each attempt — the handshake and registration deadline armed after the socket connect —
     * follows the exponential connection backoff: the windows of the consecutive failed attempts
     * must be at least half of the connect timeout, its doubled value, and its quadrupled value
     * respectively, and strictly increasing, so the backoff progression is not lost across the
     * retries.
     */
    @Test
    public void testBackoffTimeoutsIncreaseOnConsecutiveFailedAttempts() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);
            TcpCommunicationSpi spi1 = spis.get(1);

            ClusterNode node0 = nodes.get(0);
            ClusterNode node1 = nodes.get(1);

            // Establish the initial communication connection.
            CountDownLatch ack = new CountDownLatch(1);

            AtomicReference<Throwable> initErr = new AtomicReference<>();

            spi0.sendMessage(node1, new GridTestMessage(node0.id(), 1, 0), err -> {
                try {
                    initErr.compareAndSet(null, err);
                }
                finally {
                    ack.countDown();
                }
            });

            assertTrue("Failed to establish initial connection", ack.await(AWAIT_TIMEOUT, TimeUnit.SECONDS));

            assertNull("Unexpected error during initial connection", initErr.get());

            // The explicit connect timeout disables the failure detection timeout, so the
            // establishment window of the first attempt is the connect timeout itself and the
            // windows of the following attempts are the values of its exponential backoff.
            int backoffMs = 1000;

            spi0.setConnectTimeout(backoffMs);

            spi1.setConnectTimeout(backoffMs);

            GridNioServer<?> srv0 = ((GridNioServerWrapper)U.field(spi0, "nioSrvWrapper")).nio();
            GridNioServer<?> srv1 = ((GridNioServerWrapper)U.field(spi1, "nioSrvWrapper")).nio();

            // Close the established connection.
            closeAllSessions(srv0);

            closeAllSessions(srv1);

            assertTrue("Failed to wait for connection pools to become empty",
                waitForPoolsEmpty(spi0, node1, spi1, node0));

            List<Long> windowMs = new ArrayList<>();

            AtomicInteger outgoingRegistrations = new AtomicInteger();

            // The registrar holds the first three outgoing (non-accepted) registrations of node 0
            // until the establishment deadline closes the channel, and records the time from the
            // registration entry to the channel close: that is the establishment window of the
            // attempt, minus the short handshake that runs between the deadline being armed and the
            // registration. The per-attempt hard limit, far beyond the expected window, keeps the
            // worker bounded if the deadline fails to fire. All later registrations are not held, so
            // the fourth attempt establishes the connection and delivers the message.
            srv0.setChannelRegistrar((channel, selector, ops, attachment) -> {
                GridNioSession ses = (GridNioSession)attachment;

                int i = outgoingRegistrations.getAndIncrement();

                if (!ses.accepted() && i < 3) {
                    long entryNs = System.nanoTime();

                    long capMs = (long)(backoffMs << i) * 3 + 3000;

                    try {
                        while (channel.isOpen()
                            && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - entryNs) < capMs)
                            Thread.sleep(50);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    if (!channel.isOpen())
                        windowMs.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - entryNs));
                }

                return channel.register(selector, ops, attachment);
            });

            try {
                AtomicReference<Throwable> sendErr = new AtomicReference<>();

                // sendMessage blocks the calling thread until the connection (including the NIO
                // session registration) is established, so it must run on a separate thread while
                // the test thread observes the deadlines.
                Thread sendTh = startSendThread(new CyclicBarrier(1), spi0, node1, node0, 900, sendErr, null);

                sendTh.join(AWAIT_TIMEOUT * 1000L);

                assertFalse("Send thread did not finish", sendTh.isAlive());

                assertNull("Unexpected send error", sendErr.get());
            }
            finally {
                srv0.setChannelRegistrar(null);
            }

            assertEquals("Failed to observe three deadline-failed attempts", 3, windowMs.size());

            // The establishment windows of the consecutive failed attempts must follow the
            // exponential connection backoff: at least half of the connect timeout, its doubled
            // value, and its quadrupled value respectively, and strictly increasing.
            assertTrue("The establishment windows of the consecutive failed attempts do not follow "
                    + "the exponential connection backoff [windowMs=" + windowMs + ", backoffMs="
                    + backoffMs + "]",
                windowMs.get(0) >= backoffMs / 2
                    && windowMs.get(1) >= backoffMs && windowMs.get(1) > windowMs.get(0) + backoffMs / 2
                    && windowMs.get(2) >= backoffMs * 2 && windowMs.get(2) > windowMs.get(1) + backoffMs / 2);

            assertTrue("Message was not delivered over the re-established connection",
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return listeners.get(1).received(900);
                    }
                }, AWAIT_TIMEOUT * 1000L));
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Injects a pending connect future (an orphan that never completes, without any real connection
     * attempt) into the connection pool and checks that a concurrent {@code reserveClient} waiter is
     * bounded by the total reserve budget: it must give up with an {@link IgniteSpiException} after the
     * budget elapses instead of waiting indefinitely on the orphan future, and it must not remove or
     * complete the shared future.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReserveClientTimeoutWithOrphanConnectFuture() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);

            ClusterNode node1 = nodes.get(1);

            // A small, controlled reserve budget: the explicit connect timeout disables the failure
            // detection timeout, so the total reserve budget is
            // totalBackoffTimeout(connectTimeout, maxConnectTimeout, reconCount); setting the maximum
            // connect timeout equal to the connect timeout collapses that to the connect timeout itself.
            spi0.setConnectTimeout(2000);

            spi0.setMaxConnectTimeout(2000);

            ConnectionClientPool pool0 = U.field(spi0, "clientPool");

            ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
                U.field(pool0, "clientFuts");

            // An orphan connect future: pending forever, created without any real connection attempt.
            GridFutureAdapter<GridCommunicationClient> orphan = new GridFutureAdapter<>();

            ConnectionKey key = new ConnectionKey(node1.id(), 0, -1L);

            clientFuts.put(key, orphan);

            AtomicReference<Throwable> err = new AtomicReference<>();

            long[] elapsedMs = new long[1];

            Thread th = new Thread(() -> {
                long start = System.currentTimeMillis();

                try {
                    pool0.reserveClient(node1, 0);

                    err.compareAndSet(null, new IllegalStateException("reserveClient returned a client"));
                }
                catch (Throwable e) {
                    elapsedMs[0] = System.currentTimeMillis() - start;

                    err.compareAndSet(null, e);
                }
            });

            th.setName("reserve-client-timeout-self-test");

            th.setDaemon(true);

            th.start();

            th.join(AWAIT_TIMEOUT * 1000L);

            assertFalse("reserveClient did not return within a bounded time (it waited indefinitely on "
                    + "the orphan connect future)",
                th.isAlive());

            Throwable t = err.get();

            assertNotNull("reserveClient returned neither a client nor an error", t);

            assertTrue("Expected an IgniteSpiException, got: " + t, t instanceof IgniteSpiException);

            assertTrue("Unexpected error message: " + t.getMessage(),
                t.getMessage().contains("Failed to reserve a client"));

            // It must have waited until the budget elapsed (it gives up only once
            // totalReserveTimeout is exceeded), but returned in bounded time, not indefinitely.
            assertTrue("reserveClient returned before the reserve budget elapsed [elapsedMs=" + elapsedMs[0]
                    + ", budgetMs=2000]",
                elapsedMs[0] >= 1900);

            assertTrue("reserveClient did not return within a bounded time [elapsedMs=" + elapsedMs[0] + "]",
                elapsedMs[0] <= 3000);

            // The waiting caller must not remove or complete the shared future.
            assertTrue("The waiting caller removed the shared connect future from the pool",
                orphan == clientFuts.get(key));

            assertFalse("The waiting caller completed the shared connect future", orphan.isDone());
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Injects a pending connect future (an orphan that never completes) into the connection pool whose
     * per-wait timeout (derived from the workers registry's system worker blocked timeout) exceeds the
     * whole reserve budget, and establishes a client for the same node and connection index after the
     * waiter is verified from its stack trace to be blocked in the shared-future wait. By the time the
     * waiter's wait wakes it up the reserve budget has elapsed, and it must then return the established
     * client instead of giving up with the reserve timeout: the check for an already established client
     * must happen before the elapsed budget can abort the reservation, because the budget bounds waiting
     * for a connection, not the availability of an existing client.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReserveClientReturnsExistingClientAfterBudgetElapsed() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);

            ClusterNode node1 = nodes.get(1);

            // A small, controlled reserve budget: the explicit connect timeout disables the failure
            // detection timeout, so the total reserve budget is
            // totalBackoffTimeout(connectTimeout, maxConnectTimeout, reconCount); setting the maximum
            // connect timeout equal to the connect timeout collapses that to the connect timeout itself.
            spi0.setConnectTimeout(2000);

            spi0.setMaxConnectTimeout(2000);

            ConnectionClientPool pool0 = U.field(spi0, "clientPool");

            // A per-wait timeout larger than the whole reserve budget: without it the waiter would wake
            // up from its first wait long before the budget elapses (the default per-wait timeout derives
            // from the connect timeout), find the client already installed, and return it, so the case
            // under test - a client found only after the budget has elapsed - would not be reached. The
            // registry is a final field that stays null in this self test, so it is written with a
            // direct field put.
            Field registryField = ConnectionClientPool.class.getDeclaredField("registry");

            registryField.setAccessible(true);

            GridUnsafe.putObjectField(pool0, GridUnsafe.objectFieldOffset(registryField),
                new WorkersRegistry(null, 9000, null));

            ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
                U.field(pool0, "clientFuts");

            ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(pool0, "clients");

            // An orphan connect future: pending forever, created without any real connection attempt.
            GridFutureAdapter<GridCommunicationClient> orphan = new GridFutureAdapter<>();

            ConnectionKey key = new ConnectionKey(node1.id(), 0, -1L);

            clientFuts.put(key, orphan);

            // The client that a concurrent thread establishes while the waiter is still waiting; it
            // must be returned even though the reserve budget has elapsed by then.
            GridCommunicationClient established = mock(GridCommunicationClient.class);

            when(established.connectionIndex()).thenReturn(0);

            when(established.reserve()).thenReturn(true);

            AtomicReference<GridCommunicationClient> result = new AtomicReference<>();

            AtomicReference<Throwable> err = new AtomicReference<>();

            long[] elapsedMs = new long[1];

            Thread th = new Thread(() -> {
                long start = System.currentTimeMillis();

                try {
                    result.set(pool0.reserveClient(node1, 0));
                }
                catch (Throwable e) {
                    err.compareAndSet(null, e);
                }
                finally {
                    elapsedMs[0] = System.currentTimeMillis() - start;
                }
            });

            th.setName("reserve-client-existing-after-budget-self-test");

            th.setDaemon(true);

            th.start();

            // Install the client only after it is verified that the waiter is actually blocked in the
            // shared-future wait (reaching reserveClient() alone is not enough: the client must be
            // established while the waiter is waiting, not before it starts waiting): the per-wait
            // timeout exceeds the reserve budget, so the waiter stays in its first wait until the
            // budget has elapsed and only then finds the client.
            long readyDeadlineMs = System.currentTimeMillis() + AWAIT_TIMEOUT * 1000L;

            while (!isWaitingOnConnectFuture(th) && System.currentTimeMillis() < readyDeadlineMs)
                Thread.sleep(10);

            assertTrue("The client was installed before the waiter was blocked in the shared-future "
                    + "wait, so the case under test was not reached",
                isWaitingOnConnectFuture(th));

            clients.put(node1.id(), new GridCommunicationClient[]{established});

            th.join(AWAIT_TIMEOUT * 1000L);

            assertFalse("reserveClient did not return within a bounded time", th.isAlive());

            assertNull("reserveClient gave up with the reserve timeout instead of returning the "
                    + "client established in the meantime",
                err.get());

            // The mock does not override equals, so this is an identity check.
            assertEquals("reserveClient did not return the client established in the meantime",
                established, result.get());

            // The client was installed while the waiter was already blocked in the shared-future wait,
            // and the per-wait timeout exceeds the reserve budget: the waiter therefore only re-checked
            // for an established client after the budget had elapsed.
            assertTrue("reserveClient returned the client before the reserve budget elapsed "
                    + "[elapsedMs=" + elapsedMs[0] + ", budgetMs=2000]",
                elapsedMs[0] >= 1900);

            assertTrue("reserveClient did not return within a bounded time [elapsedMs=" + elapsedMs[0] + "]",
                elapsedMs[0] <= 3000);

            // The waiting caller must not remove or complete the shared future.
            assertTrue("The waiting caller removed the shared connect future from the pool",
                orphan == clientFuts.get(key));

            assertFalse("The waiting caller completed the shared connect future", orphan.isDone());
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Injects a pending connect future (an orphan that never completes) into the connection pool with a
     * workers registry whose system worker blocked timeout is disabled, so that the pool's per-wait
     * timeout is 0. Checks that a concurrent {@code reserveClient} waiter is not aborted immediately by
     * the zero per-wait timeout but waits until the total reserve budget elapses before giving up.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReserveClientTimeoutWithDisabledWorkerBlockedTimeout() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);

            ClusterNode node1 = nodes.get(1);

            // A small, controlled reserve budget: the explicit connect timeout disables the failure
            // detection timeout, and setting the maximum connect timeout equal to the connect timeout
            // collapses the total backoff timeout to the connect timeout itself.
            spi0.setConnectTimeout(2000);

            spi0.setMaxConnectTimeout(2000);

            ConnectionClientPool pool0 = U.field(spi0, "clientPool");

            ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
                U.field(pool0, "clientFuts");

            // A non-positive system worker blocked timeout is normalized to infinite, so the registry
            // reports a blocked timeout of 0 and the pool's per-wait timeout derives to 0, the case
            // under test. The registry is a final field that stays null in this self test, so it is
            // written with a direct field put.
            Field registryField = ConnectionClientPool.class.getDeclaredField("registry");

            registryField.setAccessible(true);

            GridUnsafe.putObjectField(pool0, GridUnsafe.objectFieldOffset(registryField),
                new WorkersRegistry(null, 0, null));

            // An orphan connect future: pending forever, created without any real connection attempt.
            GridFutureAdapter<GridCommunicationClient> orphan = new GridFutureAdapter<>();

            ConnectionKey key = new ConnectionKey(node1.id(), 0, -1L);

            clientFuts.put(key, orphan);

            AtomicReference<Throwable> err = new AtomicReference<>();

            long[] elapsedMs = new long[1];

            Thread th = new Thread(() -> {
                long start = System.currentTimeMillis();

                try {
                    pool0.reserveClient(node1, 0);

                    err.compareAndSet(null, new IllegalStateException("reserveClient returned a client"));
                }
                catch (Throwable e) {
                    elapsedMs[0] = System.currentTimeMillis() - start;

                    err.compareAndSet(null, e);
                }
            });

            th.setName("reserve-client-disabled-blocked-timeout-self-test");

            th.setDaemon(true);

            th.start();

            th.join(AWAIT_TIMEOUT * 1000L);

            assertFalse("reserveClient did not return within a bounded time (it waited indefinitely on "
                    + "the orphan connect future)",
                th.isAlive());

            Throwable t = err.get();

            assertNotNull("reserveClient returned neither a client nor an error", t);

            assertTrue("Expected an IgniteSpiException, got: " + t, t instanceof IgniteSpiException);

            assertTrue("Unexpected error message: " + t.getMessage(),
                t.getMessage().contains("Failed to reserve a client"));

            // The zero per-wait timeout must not abort the reservation immediately: the waiter falls
            // back to the full remaining reserve budget and only gives up once it has elapsed.
            assertTrue("reserveClient aborted on the zero per-wait timeout before the reserve budget "
                    + "elapsed [elapsedMs=" + elapsedMs[0] + ", budgetMs=2000]",
                elapsedMs[0] >= 1900);

            assertTrue("reserveClient did not return within a bounded time [elapsedMs=" + elapsedMs[0] + "]",
                elapsedMs[0] <= 3000);

            // The waiting caller must not remove or complete the shared future.
            assertTrue("The waiting caller removed the shared connect future from the pool",
                orphan == clientFuts.get(key));

            assertFalse("The waiting caller completed the shared connect future", orphan.isDone());
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Injects a pending {@link ConnectFuture} into the connection pool and has several threads reserve
     * a client against it, so that they all become waiters on the same future. Calling {@code stop()}
     * must then complete the pending future and release every waiter promptly with a stop error, instead
     * of making them wait until the total reserve budget, and it must not publish a client.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopReleasesWaitersOnPendingConnectFuture() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);

            ClusterNode node1 = nodes.get(1);

            // A large reserve budget, so that without the stop() completion the waiters would have to
            // wait the whole budget instead of being released by stop().
            spi0.setConnectTimeout(30000);

            spi0.setMaxConnectTimeout(30000);

            ConnectionClientPool pool0 = U.field(spi0, "clientPool");

            ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
                U.field(pool0, "clientFuts");

            ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(pool0, "clients");

            // A pending connect future, created without any real connection attempt.
            ConnectFuture pending = new ConnectFuture();

            ConnectionKey key = new ConnectionKey(node1.id(), 0, -1L);

            clientFuts.put(key, pending);

            final int waiters = 3;

            long releaseAwaitSecs = 5;

            CountDownLatch released = new CountDownLatch(waiters);

            Thread[] waiterThreads = new Thread[waiters];

            Throwable[] errors = new Throwable[waiters];

            long[] elapsedMs = new long[waiters];

            for (int i = 0; i < waiters; i++) {
                final int idx = i;

                Thread th = new Thread(() -> {
                    long start = System.currentTimeMillis();

                    try {
                        GridCommunicationClient client = pool0.reserveClient(node1, 0);

                        errors[idx] = new IllegalStateException("reserveClient returned a client: " + client);
                    }
                    catch (Throwable e) {
                        elapsedMs[idx] = System.currentTimeMillis() - start;

                        errors[idx] = e;
                    }
                    finally {
                        released.countDown();
                    }
                });

                waiterThreads[i] = th;

                th.setName("stop-release-waiter-" + i);

                th.setDaemon(true);

                th.start();
            }

            // Wait until every waiter is actually blocked in the shared-future wait of reserveClient,
            // so that stop() releases them from that wait instead of from an earlier check. Reaching
            // reserveClient() alone is not enough: a thread that has not entered the future wait yet
            // can still be rejected by the stopping flag that stop() sets.
            int inFutWait = 0;

            long readyDeadlineMs = System.currentTimeMillis() + releaseAwaitSecs * 1000L;

            while (System.currentTimeMillis() < readyDeadlineMs) {
                inFutWait = 0;

                for (Thread th : waiterThreads) {
                    if (isWaitingOnConnectFuture(th))
                        inFutWait++;
                }

                if (inFutWait == waiters)
                    break;

                Thread.sleep(10);
            }

            assertTrue("Not all waiters were blocked in the shared-future wait before stop() was called "
                    + "[inFutWait=" + inFutWait + ", waiters=" + waiters + ']',
                inFutWait == waiters);

            pool0.stop();

            // All waiters must be released promptly by stop(), well before the reserve budget.
            boolean allReleased = released.await(releaseAwaitSecs, TimeUnit.SECONDS);

            assertTrue("Not all waiters were released by stop() within " + releaseAwaitSecs
                    + "s (they kept waiting toward the reserve budget)",
                allReleased);

            for (int i = 0; i < waiters; i++) {
                Throwable t = errors[i];

                assertNotNull("Waiter " + i + " returned neither a client nor an error", t);

                // The specific stop-completion message proves the waiter was released by stop()
                // completing the pending future, not by the reserve budget or the stopping check.
                assertTrue("Waiter " + i + " was not released by the stop() future completion, got: " + t,
                    t.getMessage() != null && t.getMessage().contains("SPI is being stopped"));

                assertTrue("Waiter " + i + " was not released promptly [elapsedMs=" + elapsedMs[i]
                        + ", budgetMs=30000]",
                    elapsedMs[i] < releaseAwaitSecs * 1000L);
            }

            assertTrue("The pending connect future was not completed by stop()", pending.isDone());

            assertNull("A client was published to node " + node1 + " after the shutdown",
                clients.get(node1.id()));
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Stalls the NIO session registration of a new outgoing connection, holds it until the pool is
     * stopped, and only then releases it: the creating thread therefore cannot complete the shared
     * connect future before stop() does, so stop() is guaranteed to win the completion race. When the
     * creating thread then tries to publish its client, it must find that stop() already completed the
     * future with a stop error: the creating thread must not publish or return its client (the fast path
     * is taken only when it won the completion race), and it has to be released with the stop error like
     * any other waiter.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopWinsRaceAgainstCreatorCompletion() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);

            ClusterNode node1 = nodes.get(1);

            // Large connect timeout: the connection establishment deadline must not fire during the
            // stalled registration, so that only the explicit stop() can complete the shared connect future.
            spi0.setConnectTimeout(30000);

            spis.get(1).setConnectTimeout(30000);

            ConnectionClientPool pool0 = U.field(spi0, "clientPool");

            ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(pool0, "clients");

            GridNioServer<?> srv0 = ((GridNioServerWrapper)U.field(spi0, "nioSrvWrapper")).nio();

            AtomicInteger outgoingRegistrations = new AtomicInteger();

            // The registrar signals when the creating thread enters the stalled registration and then
            // holds the registration until the test releases it: the creating thread therefore cannot
            // complete the shared connect future before stop() does, so stop() wins the completion race
            // no matter how long the test thread takes between the two calls. The registration is then
            // completed normally, so the creating thread does reach the onDone(client0) call with a
            // valid client.
            CountDownLatch registrationEntered = new CountDownLatch(1);

            CountDownLatch releaseRegistration = new CountDownLatch(1);

            srv0.setChannelRegistrar((channel, selector, ops, attachment) -> {
                GridNioSession ses = (GridNioSession)attachment;

                if (!ses.accepted() && outgoingRegistrations.getAndIncrement() == 0) {
                    registrationEntered.countDown();

                    try {
                        releaseRegistration.await(AWAIT_TIMEOUT, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                return channel.register(selector, ops, attachment);
            });

            try {
                GridCommunicationClient[] returned = new GridCommunicationClient[1];

                Throwable[] error = new Throwable[1];

                CountDownLatch done = new CountDownLatch(1);

                // The creating thread: the one that inserts the shared connect future and establishes the
                // client, i.e. the candidate for the own-client fast path.
                Thread creator = new Thread(() -> {
                    try {
                        returned[0] = pool0.reserveClient(node1, 0);
                    }
                    catch (Throwable t) {
                        error[0] = t;
                    }
                    finally {
                        done.countDown();
                    }
                });

                creator.setName("stop-race-creator");

                creator.setDaemon(true);

                creator.start();

                // Wait until the creating thread is stalled in the session registration, i.e. the shared
                // connect future is present and still pending.
                assertTrue("Failed to reach the stalled session registration of the creating thread",
                    registrationEntered.await(AWAIT_TIMEOUT, TimeUnit.SECONDS));

                // Stop the pool while the registration is still held: stop() wins the completion race.
                // Only then release the registration, so that the creating thread reaches onDone(client0)
                // and finds the future already completed with the stop error.
                pool0.stop();

                releaseRegistration.countDown();

                assertTrue("Creating thread did not finish after the stalled registration was released",
                    done.await(AWAIT_TIMEOUT, TimeUnit.SECONDS));

                assertNull("The creating thread returned a client although stop() completed the shared future",
                    returned[0]);

                assertNotNull("The creating thread returned neither a client nor an error", error[0]);

                // The specific stop-completion message proves the creating thread was released by stop()
                // completing the future (the lost completion race), not by the reserve budget.
                assertTrue("The creating thread was not released by the stop() future completion, got: " + error[0],
                    error[0].getMessage() != null && error[0].getMessage().contains("SPI is being stopped"));

                GridCommunicationClient[] nodeClients = clients.get(node1.id());

                assertNull("A client was published to node " + node1 + " after the shutdown",
                    nodeClients == null ? null : nodeClients[0]);
            }
            finally {
                // Release the registration even if an assertion above failed, so that the NIO worker
                // is not left stalled.
                releaseRegistration.countDown();

                srv0.setChannelRegistrar(null);
            }
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Checks the inverse (peer-initiated) connection wait of
     * {@code ConnectionClientPool.handleUnreachableNodeException}: the inverse-connection requestor must be
     * triggered exactly once, and the subsequent wait for the inverse connection must be bounded by its
     * own window (the connect timeout, when the failure detection timeout is disabled) instead of the
     * reservation budget: a requestor that consumes the entire reserve budget must not shrink the wait,
     * and the whole path must still fail in bounded time when the inverse connection never arrives.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReserveClientInverseConnectionWithExhaustedReserveBudget() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);

            ClusterNode node1 = nodes.get(1);

            // A small, controlled window: the explicit connect timeout disables the failure
            // detection timeout, so both the total reserve budget and the inverse-connection wait
            // window are the connect timeout itself (setting the maximum connect timeout equal to
            // the connect timeout collapses the total backoff timeout to it).
            spi0.setConnectTimeout(1000);

            spi0.setMaxConnectTimeout(1000);

            ConnectionClientPool pool0 = U.field(spi0, "clientPool");

            ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
                U.field(pool0, "clientFuts");

            // The private method under test.
            Method handle = ConnectionClientPool.class.getDeclaredMethod("handleUnreachableNodeException",
                ClusterNode.class, int.class, GridFutureAdapter.class, NodeUnreachableException.class);

            handle.setAccessible(true);

            AtomicInteger requests = new AtomicInteger();

            // The requestor records the trigger and consumes more than the entire reserve budget
            // before returning, but never establishes the inverse connection itself: the wait for it
            // must therefore still get its own full window (the connect timeout), not the budget that
            // the request call has already exhausted.
            spi0.setConnectionRequestor((node, connIdx) -> {
                requests.incrementAndGet();

                try {
                    Thread.sleep(1200);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            AtomicReference<Throwable> err = new AtomicReference<>();

            long[] elapsedMs = new long[1];

            Thread th = new Thread(() -> {
                long start = System.nanoTime();

                try {
                    handle.invoke(pool0, node1, 0, new ConnectFuture(),
                        new NodeUnreachableException("inverse connection"));

                    err.set(new IllegalStateException("handleUnreachableNodeException returned normally"));
                }
                catch (InvocationTargetException e) {
                    elapsedMs[0] = (System.nanoTime() - start) / 1_000_000L;

                    err.set(e.getCause());
                }
                catch (ReflectiveOperationException e) {
                    err.set(e);
                }
            });

            th.setName("reserve-client-inverse-connection-self-test");

            th.setDaemon(true);

            th.start();

            th.join(AWAIT_TIMEOUT * 1000L);

            assertFalse("The inverse-connection path did not return within a bounded time", th.isAlive());

            Throwable t = err.get();

            assertNotNull("The inverse-connection path returned neither an error", t);

            assertTrue("Expected an IgniteSpiException, got: " + t, t instanceof IgniteSpiException);

            assertTrue("Unexpected cause (not the failed inverse-connection wait): " + t.getCause(),
                t.getCause() instanceof NodeUnreachableException);

            assertEquals("The inverse-connection requestor was not triggered exactly once",
                1, requests.get());

            // The requestor consumed ~1200 ms, more than the 1000 ms reserve budget: the wait must
            // still have used its own full 1000 ms window (the connect timeout), so the whole path
            // takes about 2200 ms. Capping the wait by the (already exhausted) reserve budget would
            // have aborted it right after the request call, at ~1200 ms. This is a wall-clock
            // regression bound on the window, not a precise measurement of the timeout that is
            // passed to the internal future wait.
            assertTrue("The inverse-connection wait was not bounded by its own window instead of the "
                    + "reserve budget [elapsedMs=" + elapsedMs[0] + ", windowMs=1000, requestMs=1200]",
                elapsedMs[0] >= 2000 && elapsedMs[0] <= 3200);

            // The trigger-future state must not leak.
            assertTrue("A trigger future leaked in the connection pool",
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return !clientFuts.containsKey(new ConnectionKey(node1.id(), 0, -1L));
                    }
                }, AWAIT_TIMEOUT * 1000L));
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Models a connection producer that has failed its direct attempts and is now in the
     * inverse-connection phase (its requestor consumes a time span far beyond the reserve budget and
     * never establishes the inverse connection) while a concurrent waiter reserves a client on the
     * producer's connect future. The waiter must be released by its own reserve budget without waiting
     * for the producer's inverse-connection phase to finish, and it must not remove or complete the
     * shared future.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReserveClientTimeoutWithProducerInInverseConnectionPhase() throws Exception {
        createSpis();

        try {
            TcpCommunicationSpi spi0 = spis.get(0);

            ClusterNode node1 = nodes.get(1);

            // A small, controlled reserve budget: the explicit connect timeout disables the failure
            // detection timeout, so the total reserve budget and the producer's inverse-connection
            // wait window are both the connect timeout itself.
            spi0.setConnectTimeout(1000);

            spi0.setMaxConnectTimeout(1000);

            ConnectionClientPool pool0 = U.field(spi0, "clientPool");

            ConcurrentMap<ConnectionKey, GridFutureAdapter<GridCommunicationClient>> clientFuts =
                U.field(pool0, "clientFuts");

            // The private method that runs the producer's inverse-connection phase.
            Method handle = ConnectionClientPool.class.getDeclaredMethod("handleUnreachableNodeException",
                ClusterNode.class, int.class, GridFutureAdapter.class, NodeUnreachableException.class);

            handle.setAccessible(true);

            // The producer's in-progress connect future, published in the pool before the producer
            // enters the inverse-connection phase.
            ConnectFuture producerFut = new ConnectFuture();

            ConnectionKey key = new ConnectionKey(node1.id(), 0, -1L);

            clientFuts.put(key, producerFut);

            AtomicInteger requests = new AtomicInteger();

            // The inverse-connection requestor stays far beyond the waiter's reserve budget and never
            // establishes the inverse connection: the waiter must be released by its own budget, not
            // by the producer's phase finishing.
            spi0.setConnectionRequestor((node, connIdx) -> {
                requests.incrementAndGet();

                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            AtomicBoolean prodReturnedNormally = new AtomicBoolean();

            AtomicReference<Throwable> prodErr = new AtomicReference<>();

            // The producer: stuck in the inverse-connection phase (the request plus its own wait
            // window, about 6000 ms in total).
            Thread prodTh = new Thread(() -> {
                try {
                    handle.invoke(pool0, node1, 0, producerFut,
                        new NodeUnreachableException("inverse connection"));

                    prodReturnedNormally.set(true);
                }
                catch (InvocationTargetException e) {
                    prodErr.set(e.getCause());
                }
                catch (ReflectiveOperationException e) {
                    prodErr.set(e);
                }
            });

            prodTh.setName("reserve-client-inverse-producer-self-test");

            prodTh.setDaemon(true);

            prodTh.start();

            // Wait until the producer has published its trigger future and entered the request call,
            // so that the waiter below starts while the producer is in the inverse-connection phase.
            assertTrue("The inverse-connection requestor was not triggered",
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return requests.get() == 1;
                    }
                }, AWAIT_TIMEOUT * 1000L));

            GridFutureAdapter<GridCommunicationClient> triggerFut = clientFuts.get(key);

            assertNotNull("The producer did not publish its trigger future", triggerFut);

            AtomicReference<Throwable> err = new AtomicReference<>();

            long[] elapsedMs = new long[1];

            Thread waiterTh = new Thread(() -> {
                long start = System.currentTimeMillis();

                try {
                    pool0.reserveClient(node1, 0);

                    err.compareAndSet(null, new IllegalStateException("reserveClient returned a client"));
                }
                catch (Throwable e) {
                    elapsedMs[0] = System.currentTimeMillis() - start;

                    err.compareAndSet(null, e);
                }
            });

            waiterTh.setName("reserve-client-producer-inverse-self-test");

            waiterTh.setDaemon(true);

            waiterTh.start();

            waiterTh.join(AWAIT_TIMEOUT * 1000L);

            assertFalse("The waiter did not return within a bounded time (it waited for the producer's "
                    + "inverse-connection phase)",
                waiterTh.isAlive());

            Throwable t = err.get();

            assertNotNull("reserveClient returned neither a client nor an error", t);

            assertTrue("Expected an IgniteSpiException, got: " + t, t instanceof IgniteSpiException);

            assertTrue("Unexpected error message: " + t.getMessage(),
                t.getMessage().contains("Failed to reserve a client"));

            // The waiter must have given up on its own reserve budget, far before the producer's
            // inverse-connection phase (~6000 ms) finished.
            assertTrue("The waiter was not released by its own reserve budget [elapsedMs=" + elapsedMs[0]
                    + ", budgetMs=1000]",
                elapsedMs[0] >= 900 && elapsedMs[0] <= 3000);

            // The waiter's release must not come from completing the shared futures: they are still
            // pending and still published at this point.
            assertFalse("The waiter was released by the producer's connect future completing instead "
                    + "of its own reserve budget",
                producerFut.isDone());

            assertTrue("The waiter removed the shared trigger future from the pool",
                triggerFut == clientFuts.get(key));

            assertFalse("The waiter completed the shared trigger future", triggerFut.isDone());

            prodTh.join(AWAIT_TIMEOUT * 1000L);

            assertFalse("The producer's inverse-connection phase did not finish", prodTh.isAlive());

            assertFalse("handleUnreachableNodeException returned normally although the inverse "
                    + "connection never arrived",
                prodReturnedNormally.get());

            assertNotNull("The producer's inverse-connection path returned neither an error", prodErr.get());

            assertTrue("Expected an IgniteSpiException from the producer, got: " + prodErr.get(),
                prodErr.get() instanceof IgniteSpiException);

            assertTrue("Unexpected producer cause (not the failed inverse-connection wait): "
                    + prodErr.get().getCause(),
                prodErr.get().getCause() instanceof NodeUnreachableException);

            assertEquals("The inverse-connection requestor was not triggered exactly once",
                1, requests.get());

            // The trigger-future state must not leak, and the producer's failed phase must have
            // completed the shared connect future with the error.
            assertTrue("A trigger future leaked in the connection pool",
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return !clientFuts.containsKey(key);
                    }
                }, AWAIT_TIMEOUT * 1000L));

            assertTrue("The producer's failed inverse-connection phase did not complete the connect "
                    + "future",
                producerFut.isDone());
        }
        finally {
            stopSpis();
        }
    }

    /**
     * @param th Thread to check.
     * @return {@code True} if the thread is blocked in the connect-future wait of
     * {@code ConnectionClientPool.reserveClient}.
     */
    private static boolean isWaitingOnConnectFuture(Thread th) {
        boolean inFutGet = false;

        for (StackTraceElement el : th.getStackTrace()) {
            if (el.getClassName().equals("org.apache.ignite.internal.util.future.GridFutureAdapter")
                && el.getMethodName().equals("get0"))
                inFutGet = true;
            else if (el.getClassName().equals("org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool")
                && el.getMethodName().equals("reserveClient"))
                return inFutGet;
        }

        return false;
    }

    /**
     * Closes all sessions of the given NIO server.
     *
     * @param srv NIO server.
     * @throws Exception If failed.
     */
    private void closeAllSessions(GridNioServer<?> srv) throws Exception {
        Set<Integer> closedIds = new HashSet<>();

        for (GridNioSession ses : srv.sessions()) {
            closedIds.add(System.identityHashCode(ses));

            ses.close();
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return srv.sessions().isEmpty();
            }
        }, AWAIT_TIMEOUT * 1000L);

        if (!srv.sessions().isEmpty()) {
            StringBuilder sb = new StringBuilder("CLOSE-TIMEOUT: remaining sessions: ");

            for (GridNioSession ses : srv.sessions()) {
                int id = System.identityHashCode(ses);

                sb.append('[').append(Integer.toHexString(id))
                    .append(closedIds.contains(id) ? "" : " NEW")
                    .append('@').append(ses.localAddress()).append('-').append(ses.remoteAddress())
                    .append(" closeTime=").append(ses.closeTime())
                    .append(" readsPaused=").append(ses.readsPaused())
                    .append("] ");
            }

            dumpNioThreads(sb);

            info(sb.toString());
        }

        assertTrue("Failed to close all sessions", srv.sessions().isEmpty());
    }

    /**
     * Appends stack traces of all NIO worker/selector threads to the given string builder (diagnostics).
     *
     * @param sb String builder.
     */
    private void dumpNioThreads(StringBuilder sb) {
        sb.append(" | nio threads: ");

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (!t.getName().contains("nio"))
                continue;

            StackTraceElement[] st = t.getStackTrace();

            sb.append('(').append(t.getName()).append(':');

            for (int i = 0; i < Math.min(6, st.length); i++) {
                sb.append(st[i].getClassName().substring(st[i].getClassName().lastIndexOf('.') + 1))
                    .append('.').append(st[i].getMethodName()).append('/').append(st[i].getLineNumber()).append(';');
            }

            sb.append(')');
        }
    }

    /**
     * @param spi0  SPI of node 1.
     * @param node1 Node 2.
     * @param spi1  SPI of node 2.
     * @param node0 Node 1.
     * @return {@code True} if both connection pools hold no live clients.
     * @throws Exception If failed.
     */
    private boolean waitForPoolsEmpty(TcpCommunicationSpi spi0, ClusterNode node1, TcpCommunicationSpi spi1,
        ClusterNode node0) throws Exception {
        return GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                ConnectionClientPool pool0 = U.field(spi0, "clientPool");
                ConnectionClientPool pool1 = U.field(spi1, "clientPool");

                GridCommunicationClient[] arr0 = pool0.clientFor(node1.id());
                GridCommunicationClient[] arr1 = pool1.clientFor(node0.id());

                return (arr0 == null || arr0[0] == null) && (arr1 == null || arr1[0] == null);
            }
        }, AWAIT_TIMEOUT * 1000L);
    }

    /**
     * Starts a thread that sends a message once the barrier is released.
     * <p>
     * The send callback is intentionally a no-op: it may run asynchronously after the send thread has
     * terminated, so the caller cannot synchronize with it, and it is not guaranteed to run at all,
     * because it depends on the recovery ack counters, which the reconnection under test desynchronizes.
     * Delivery is verified at the receiving listener. The error holder only captures a failure of the
     * {@code sendMessage} call itself, which the caller can assert on after joining the send thread.
     *
     * @param barrier        Barrier both send threads are released by.
     * @param spi            SPI to send with.
     * @param dstNode        Destination node.
     * @param srcNode        Source node.
     * @param msgId          Message ID.
     * @param err            Synchronous send error holder, capturing a failure of the {@code sendMessage} call itself.
     * @param attemptStartNs Anchor capturing the moment right before the {@code sendMessage} call, or
     *                       {@code null} when the anchor is not needed.
     * @return Started thread.
     */
    private Thread startSendThread(CyclicBarrier barrier, TcpCommunicationSpi spi, ClusterNode dstNode,
        ClusterNode srcNode, long msgId, AtomicReference<Throwable> err, @Nullable AtomicLong attemptStartNs) {
        Thread th = new Thread(() -> {
            try {
                barrier.await();

                if (attemptStartNs != null)
                    attemptStartNs.set(System.nanoTime());

                spi.sendMessage(dstNode, new GridTestMessage(srcNode.id(), msgId, 0), e -> {
                    // Intentionally ignored, see the method javadoc.
                });
            }
            catch (Exception e) {
                err.compareAndSet(null, e);
            }
        });

        th.setName("connect-closure-self-test-sender");
        th.setDaemon(true);

        th.start();

        return th;
    }

    /**
     * Communication listener tracking the IDs of the received test messages, so that delivery can be
     * verified at the receiving node.
     */
    private class TestListener implements CommunicationListener<Message> {
        /**
         * Received message IDs.
         */
        private final Set<Long> receivedMsgIds = ConcurrentHashMap.newKeySet();

        /**
         * {@inheritDoc}
         */
        @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            // Record the receipt before running the user callback, so that the tracked IDs document the
            // message arrival independently of the callback's behavior.
            if (msg instanceof GridTestMessage)
                receivedMsgIds.add(((GridTestMessage)msg).getMsgId());

            msgC.run();
        }

        /**
         * {@inheritDoc}
         */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }

        /**
         * @return Received message IDs.
         */
        public Set<Long> receivedMsgIds() {
            return receivedMsgIds;
        }

        /**
         * @param msgId Message ID.
         * @return {@code True} if a message with the given ID has been received.
         */
        public boolean received(long msgId) {
            return receivedMsgIds.contains(msgId);
        }
    }

    /**
     * @return SPI instance.
     */
    private TcpCommunicationSpi createSpi() {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(60_000);
        spi.setConnectTimeout(10_000);
        spi.setConnectionsPerNode(1);
        spi.setUsePairedConnections(false);
        spi.setTcpNoDelay(true);
        spi.setAckSendThreshold(1);

        return spi;
    }

    /**
     * @throws Exception If failed.
     */
    private void startSpis() throws Exception {
        spis.clear();
        nodes.clear();
        spiRsrcs.clear();
        listeners.clear();

        Map<ClusterNode, GridSpiTestContext> ctxs = new HashMap<>();

        timeoutProc = new GridTimeoutProcessor(new GridTestKernalContext(log));

        timeoutProc.start();

        timeoutProc.onKernalStart(true);

        for (int i = 0; i < SPI_CNT; i++) {
            TcpCommunicationSpi spi = createSpi();

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "igniteInstanceName", "grid-" + i);

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            GridSpiTestContext ctx = initSpiContext();

            ctx.messageFactory(new IgniteMessageFactoryImpl(
                new MessageFactoryProvider[] {new CoreMessagesProvider(), GRID_TEST_MESSAGE_FACTORY}
            ));

            ctx.setLocalNode(node);

            ctx.timeoutProcessor(timeoutProc);

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            if (useSsl) {
                IgniteMock ignite = GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "ignite");

                IgniteConfiguration cfg = ignite.configuration()
                    .setSslContextFactory(GridTestUtils.sslFactory());

                ignite.setStaticCfg(cfg);
            }

            TestListener lsnr = new TestListener();

            listeners.add(lsnr);

            spi.setListener(lsnr);

            node.order(i + 1);

            nodes.add(node);

            spi.spiStart(getTestIgniteInstanceName() + (i + 1));

            node.setAttributes(spi.getNodeAttributes());

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
     * @throws Exception If failed.
     */
    private void createSpis() throws Exception {
        for (int i = 0; i < 3; i++) {
            try {
                startSpis();

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
        if (timeoutProc != null) {
            timeoutProc.onKernalStop(true);

            timeoutProc.stop(true);

            timeoutProc = null;
        }

        for (TcpCommunicationSpi spi : spis) {
            spi.onContextDestroyed();

            spi.setListener(null);

            spi.spiStop();
        }

        for (IgniteTestResources rsrcs : spiRsrcs)
            rsrcs.stopThreads();

        spis.clear();
        nodes.clear();
        spiRsrcs.clear();
        listeners.clear();
    }

    /**
     * A {@link SocketChannel} that forwards every operation to the real channel and only provides a
     * {@link SlowConnectSocket} as its socket: the real channel keeps its own state machine, so a
     * connect through the slow socket behaves like a slow network connection.
     */
    private static final class SlowConnectChannel extends SocketChannel {
        /** Real channel. */
        private final SocketChannel real;

        /** Slow socket. */
        private final SlowConnectSocket slowSocket;

        /**
         * @param real Real channel to forward operations to.
         * @param slowConnects Counter of slow connects.
         * @param slowConnectMs Connect delay in milliseconds.
         * @throws IOException If the real channel does not provide a socket.
         */
        private SlowConnectChannel(SocketChannel real, AtomicInteger slowConnects, int slowConnectMs) throws IOException {
            super(SelectorProvider.provider());

            this.real = real;
            this.slowSocket = new SlowConnectSocket(real.socket(), slowConnects, slowConnectMs);
        }

        /** {@inheritDoc} */
        @Override public Socket socket() {
            return slowSocket;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer dst) throws IOException {
            return real.read(dst);
        }

        /** {@inheritDoc} */
        @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return real.read(dsts, offset, length);
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer src) throws IOException {
            return real.write(src);
        }

        /** {@inheritDoc} */
        @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return real.write(srcs, offset, length);
        }

        /** {@inheritDoc} */
        @Override public SocketChannel bind(SocketAddress local) throws IOException {
            return real.bind(local);
        }

        /** {@inheritDoc} */
        @Override public boolean connect(SocketAddress remote) throws IOException {
            return real.connect(remote);
        }

        /** {@inheritDoc} */
        @Override public boolean finishConnect() throws IOException {
            return real.finishConnect();
        }

        /** {@inheritDoc} */
        @Override public SocketChannel shutdownInput() throws IOException {
            return real.shutdownInput();
        }

        /** {@inheritDoc} */
        @Override public SocketChannel shutdownOutput() throws IOException {
            return real.shutdownOutput();
        }

        /** {@inheritDoc} */
        @Override public boolean isConnected() {
            return real.isConnected();
        }

        /** {@inheritDoc} */
        @Override public boolean isConnectionPending() {
            return real.isConnectionPending();
        }

        /** {@inheritDoc} */
        @Override public SocketAddress getRemoteAddress() throws IOException {
            return real.getRemoteAddress();
        }

        /** {@inheritDoc} */
        @Override public SocketAddress getLocalAddress() throws IOException {
            return real.getLocalAddress();
        }

        /** {@inheritDoc} */
        @Override public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
            return real.setOption(name, value);
        }

        /** {@inheritDoc} */
        @Override public <T> T getOption(SocketOption<T> name) throws IOException {
            return real.getOption(name);
        }

        /** {@inheritDoc} */
        @Override public Set<SocketOption<?>> supportedOptions() {
            return real.supportedOptions();
        }

        /** {@inheritDoc} */
        @Override protected void implCloseSelectableChannel() throws IOException {
            real.close();
        }

        /** {@inheritDoc} */
        @Override protected void implConfigureBlocking(boolean block) throws IOException {
            real.configureBlocking(block);
        }
    }

    /**
     * A {@link Socket} that forwards to the real socket of the channel and delays the first connect.
     */
    private static final class SlowConnectSocket extends Socket {
        /** Real socket. */
        private final Socket real;

        /** Counter of slow connects. */
        private final AtomicInteger slowConnects;

        /** Connect delay in milliseconds. */
        private final int slowConnectMs;

        /**
         * @param real Real socket to forward to.
         * @param slowConnects Counter of slow connects.
         * @param slowConnectMs Connect delay in milliseconds.
         * @throws IOException If the unused socket of the default constructor cannot be closed.
         */
        private SlowConnectSocket(Socket real, AtomicInteger slowConnects, int slowConnectMs) throws IOException {
            super();

            this.real = real;
            this.slowConnects = slowConnects;
            this.slowConnectMs = slowConnectMs;

            // Close the unused socket allocated by the default constructor.
            close();
        }

        /** {@inheritDoc} */
        @Override public void connect(SocketAddress endpoint, int timeout) throws IOException {
            if (slowConnects.incrementAndGet() == 1) {
                try {
                    Thread.sleep(slowConnectMs);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new InterruptedIOException("Interrupted during slow connect.");
                }
            }

            real.connect(endpoint, timeout);
        }

        /** {@inheritDoc} */
        @Override public void setTcpNoDelay(boolean on) throws SocketException {
            real.setTcpNoDelay(on);
        }

        /** {@inheritDoc} */
        @Override public void setKeepAlive(boolean on) throws SocketException {
            real.setKeepAlive(on);
        }

        /** {@inheritDoc} */
        @Override public void setReceiveBufferSize(int size) throws SocketException {
            real.setReceiveBufferSize(size);
        }

        /** {@inheritDoc} */
        @Override public void setSendBufferSize(int size) throws SocketException {
            real.setSendBufferSize(size);
        }

        /** {@inheritDoc} */
        @Override public int getSendBufferSize() throws SocketException {
            return real.getSendBufferSize();
        }

        /** {@inheritDoc} */
        @Override public int getReceiveBufferSize() throws SocketException {
            return real.getReceiveBufferSize();
        }
    }
}
