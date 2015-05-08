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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.*;
import org.apache.ignite.testframework.junits.spi.*;
import org.eclipse.jetty.util.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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
                @Override
                public Long call() throws Exception {
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
                    GridTestUtils.runMultiThreaded(new Callable<Void>() {
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
                    }, threads, "test");

                    assertTrue(latch.await(10, TimeUnit.SECONDS));

                    for (CommunicationSpi spi : spis) {
                        ConcurrentMap<UUID, GridTcpCommunicationClient> clients = U.field(spi, "clients");

                        assertEquals(1, clients.size());

                        final GridNioServer srv = U.field(spi, "nioSrvr");

                        GridTestUtils.waitForCondition(new GridAbsPredicate() {
                            @Override public boolean apply() {
                                Collection sessions = U.field(srv, "sessions");

                                return sessions.size() == 1;
                            }
                        }, 5000);

                        Collection sessions = U.field(srv, "sessions");

                        assertEquals(1, sessions.size());
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

            GridTestUtils.setFieldValue(spi, "gridName", "grid-" + i);

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            node.order(i + 1);

            GridSpiTestContext ctx = initSpiContext();

            ctx.setLocalNode(node);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

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
