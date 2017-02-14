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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.typedef.CO;
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
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

/**
 *
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class GridTcpCommunicationSpiConcurrentConnectSelfTest<T extends CommunicationSpi>
    extends GridSpiAbstractTest<T> {
    /** */
    private static final int SPI_CNT = 2;

    /** */
    private static final int ITERS = 50;

    /** */
    private static final Collection<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /** */
    protected static final List<CommunicationSpi<Message>> spis = new ArrayList<>();

    /** */
    protected static final List<ClusterNode> nodes = new ArrayList<>();

    /** */
    private static int port = 60_000;

    /** Use ssl. */
    protected boolean useSsl;

    /** */
    private int connectionsPerNode = 1;

    /** */
    private boolean pairedConnections = true;

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
    public GridTcpCommunicationSpiConcurrentConnectSelfTest() {
        super(false);
    }

    /**
     *
     */
    private static class MessageListener implements CommunicationListener<Message> {
        /** */
        private final CountDownLatch latch;

        /** */
        private final AtomicInteger cntr = new AtomicInteger();

        /** */
        private final ConcurrentHashSet<Long> msgIds = new ConcurrentHashSet<>();

        /**
         * @param latch Latch.
         */
        MessageListener(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            msgC.run();

            assertTrue(msg instanceof GridTestMessage);

            cntr.incrementAndGet();

            GridTestMessage msg0 = (GridTestMessage)msg;

            assertEquals(nodeId, msg0.getSourceNodeId());

            assertTrue(msgIds.add(msg0.getMsgId()));

            latch.countDown();
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoThreads() throws Exception {
        concurrentConnect(2, 10, ITERS, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreaded() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors() * 5;

        concurrentConnect(threads, 10, ITERS, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreaded_10Connections() throws Exception {
        connectionsPerNode = 10;

        testMultithreaded();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreaded_NoPairedConnections() throws Exception {
        pairedConnections = false;

        testMultithreaded();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreaded_10ConnectionsNoPaired() throws Exception {
        pairedConnections = false;
        connectionsPerNode = 10;

        testMultithreaded();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithLoad() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors() * 5;

        concurrentConnect(threads, 10, ITERS / 2, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomSleep() throws Exception {
        concurrentConnect(4, 1, ITERS, true, false);
    }

    /**
     * @param threads Number of threads.
     * @param msgPerThread Messages per thread.
     * @param iters Number of iterations.
     * @param sleep If {@code true} sleeps random time before starts send messages.
     * @param load Run load threads flag.
     * @throws Exception If failed.
     */
    private void concurrentConnect(final int threads,
        final int msgPerThread,
        final int iters,
        final boolean sleep,
        boolean load) throws Exception {
        log.info("Concurrent connect [threads=" + threads +
            ", msgPerThread=" + msgPerThread +
            ", iters=" + iters +
            ", load=" + load +
            ", sleep=" + sleep + ']');

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = null;

        if (load) {
            loadFut = GridTestUtils.runMultiThreadedAsync(new Callable<Long>() {
                @Override public Long call() throws Exception {
                    long dummyRes = 0;

                    List<String> list = new ArrayList<>();

                    while (!stop.get()) {
                        for (int i = 0; i < 100; i++) {
                            String str = new String(new byte[i]);

                            list.add(str);

                            dummyRes += str.hashCode();
                        }

                        if (list.size() > 1000_000) {
                            list = new ArrayList<>();

                            System.gc();
                        }
                    }

                    return dummyRes;
                }
            }, 2, "test-load");
        }

        try {
            for (int i = 0; i < iters; i++) {
                log.info("Iteration: " + i);

                final AtomicInteger msgId = new AtomicInteger();

                final int expMsgs = threads * msgPerThread;

                CountDownLatch latch = new CountDownLatch(expMsgs);

                MessageListener lsnr = new MessageListener(latch);

                createSpis(lsnr);

                final AtomicInteger idx = new AtomicInteger();

                try {
                    final Callable<Void> c = new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            int idx0 = idx.getAndIncrement();

                            Thread.currentThread().setName("Test thread [idx=" + idx0 + ", grid=" + (idx0 % 2) + ']');

                            CommunicationSpi<Message> spi = spis.get(idx0 % 2);

                            ClusterNode srcNode = nodes.get(idx0 % 2);

                            ClusterNode dstNode = nodes.get((idx0 + 1) % 2);

                            if (sleep) {
                                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                                long millis = rnd.nextLong(10);

                                if (millis > 0)
                                    Thread.sleep(millis);
                            }

                            for (int i = 0; i < msgPerThread; i++)
                                spi.sendMessage(dstNode, new GridTestMessage(srcNode.id(), msgId.incrementAndGet(), 0));

                            return null;
                        }
                    };

                    List<Thread> threadsList = new ArrayList<>();

                    final AtomicBoolean fail = new AtomicBoolean();

                    final AtomicLong tId = new AtomicLong();

                    for (int t = 0; t < threads; t++) {
                        Thread t0 = new Thread(new Runnable() {
                            @Override public void run() {
                                try {
                                    c.call();
                                }
                                catch (Throwable e) {
                                    log.error("Unexpected error: " + e, e);

                                    fail.set(true);
                                }
                            }
                        }) {
                            @Override public long getId() {
                                // Override getId to use all connections.
                                return tId.getAndIncrement();
                            }
                        };

                        threadsList.add(t0);

                        t0.start();
                    }

                    for (Thread t0 : threadsList)
                        t0.join();

                    assertTrue(latch.await(10, TimeUnit.SECONDS));

                    for (CommunicationSpi spi : spis) {
                        ConcurrentMap<UUID, GridCommunicationClient> clients = U.field(spi, "clients");

                        assertEquals(1, clients.size());

                        final GridNioServer srv = U.field(spi, "nioSrvr");

                        final int conns = pairedConnections ? 2 : 1;

                        GridTestUtils.waitForCondition(new GridAbsPredicate() {
                            @Override public boolean apply() {
                                Collection sessions = U.field(srv, "sessions");

                                return sessions.size() == conns * connectionsPerNode;
                            }
                        }, 5000);

                        Collection sessions = U.field(srv, "sessions");

                        assertEquals(conns * connectionsPerNode, sessions.size());
                    }

                    assertEquals(expMsgs, lsnr.cntr.get());
                }
                finally {
                    stopSpis();
                }
            }
        }
        finally {
            stop.set(true);

            if (loadFut != null)
                loadFut.get();
        }
    }

    /**
     * @return SPI.
     */
    private CommunicationSpi createSpi() {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setLocalAddress("127.0.0.1");
        spi.setLocalPort(port++);
        spi.setIdleConnectionTimeout(60_000);
        spi.setConnectTimeout(10_000);
        spi.setSharedMemoryPort(-1);
        spi.setConnectionsPerNode(connectionsPerNode);
        spi.setUsePairedConnections(pairedConnections);

        return spi;
    }

    /**
     * @param lsnr Message listener.
     * @throws Exception If failed.
     */
    private void startSpis(MessageListener lsnr) throws Exception {
        spis.clear();
        nodes.clear();
        spiRsrcs.clear();

        Map<ClusterNode, GridSpiTestContext> ctxs = new HashMap<>();

        for (int i = 0; i < SPI_CNT; i++) {
            CommunicationSpi<Message> spi = createSpi();

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "gridName", "grid-" + i);

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            node.order(i + 1);

            GridSpiTestContext ctx = initSpiContext();

            ctx.setLocalNode(node);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            if (useSsl) {
                IgniteMock ignite = GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "ignite");

                IgniteConfiguration cfg = ignite.configuration()
                    .setSslContextFactory(GridTestUtils.sslFactory());

                ignite.setStaticCfg(cfg);
            }

            spi.setListener(lsnr);

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
     * @param lsnr Message listener.
     * @throws Exception If failed.
     */
    private void createSpis(MessageListener lsnr) throws Exception {
        for (int i = 0; i < 3; i++) {
            try {
                startSpis(lsnr);

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
    }

}
