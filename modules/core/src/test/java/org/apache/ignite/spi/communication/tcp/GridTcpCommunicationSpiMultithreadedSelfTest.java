/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.communication.tcp;

import mx4j.tools.adaptor.http.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.communication.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.spi.*;
import org.jdk8.backport.*;

import javax.management.*;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Class for multithreaded {@link TcpCommunicationSpi} test.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public abstract class GridTcpCommunicationSpiMultithreadedSelfTest extends GridSpiAbstractTest<TcpCommunicationSpi> {
    /** Connection idle timeout */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** Thread count for testFlowSend test. */
    public static final int THREAD_CNT = 20;

    /** Message id sequence. */
    private AtomicLong msgId = new AtomicLong();

    /** */
    private final boolean useShmem;

    /** SPI resources. */
    private static final Collection<GridTestResources> spiRsrcs = new ArrayList<>();

    /** SPIs */
    private static final Map<UUID, CommunicationSpi<GridTcpCommunicationMessageAdapter>> spis =
        new ConcurrentHashMap<>();

    /** Listeners. */
    private static final Map<UUID, MessageListener> lsnrs = new HashMap<>();

    /** Initialized nodes */
    private static final List<ClusterNode> nodes = new ArrayList<>();

    /** */
    private static final ObjectName mBeanName;

    /** Flag indicating if listener should reject messages. */
    private static boolean reject;

    static {
        GridTcpCommunicationMessageFactory.registerCustom(new GridTcpCommunicationMessageProducer() {
            @Override public GridTcpCommunicationMessageAdapter create(byte type) {
                return new GridTestMessage();
            }
        }, GridTestMessage.DIRECT_TYPE);

        try {
            mBeanName = new ObjectName("mbeanAdaptor:protocol=HTTP");
        }
        catch (MalformedObjectNameException e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     * @param useShmem Use shared mem.
     */
    protected GridTcpCommunicationSpiMultithreadedSelfTest(boolean useShmem) {
        super(false);

        this.useShmem = useShmem;
    }

    /**
     * Accumulating listener.
     */
    @SuppressWarnings({"deprecation"})
    private static class MessageListener implements CommunicationListener<GridTcpCommunicationMessageAdapter> {
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
        @Override public void onMessage(UUID nodeId, GridTcpCommunicationMessageAdapter msg, IgniteRunnable msgC) {
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

        final ConcurrentMap<UUID, ConcurrentLinkedDeque8<GridTestMessage>> msgs =
            new ConcurrentHashMap<>();

        final int iterationCnt = 5000;

        long start = System.currentTimeMillis();

        IgniteFuture<?> fut = multithreadedAsync(new Runnable() {
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
                catch (GridException e) {
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

        IgniteFuture<?> fut = multithreadedAsync(new Runnable() {
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
                                GridTcpCommunicationMessageAdapter msg =
                                    new GridTestMessage(from.id(), msgId.getAndIncrement(), 0);

                                spis.get(from.id()).sendMessage(node, msg);
                            }
                        }
                        catch (GridException e) {
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

        IgniteFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    while (run.get() && !Thread.currentThread().isInterrupted()) {
                        U.sleep(interval * 3 / 2);

                        ((TcpCommunicationSpi)spis.get(from.id())).onNodeLeft(to.id());
                    }
                }
                catch (GridInterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }, 1);

        fut.get();

        run.set(false);

        fut2.get();
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

        IgniteFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    ClusterNode from = nodes.get(0);

                    ClusterNode to = nodes.get(1);

                    CommunicationSpi<GridTcpCommunicationMessageAdapter> spi = spis.get(from.id());

                    while (cntr.getAndIncrement() < msgCnt) {
                        GridTestMessage msg = new GridTestMessage(from.id(), msgId.getAndIncrement(), 0);

                        msg.payload(new byte[10 * 1024]);

                        spi.sendMessage(to, msg);
                    }
                }
                catch (GridException e) {
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
    private CommunicationSpi<GridTcpCommunicationMessageAdapter> newCommunicationSpi() {
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

        for (int i = 0; i < getSpiCount(); i++) {
            CommunicationSpi<GridTcpCommunicationMessageAdapter> spi = newCommunicationSpi();

            GridTestResources rsrcs = new GridTestResources(getMBeanServer(i));

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            GridSpiTestContext ctx = initSpiContext();

            ctx.setLocalNode(node);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            MessageListener lsnr = new MessageListener(rsrcs.getNodeId());

            spi.setListener(lsnr);

            lsnrs.put(rsrcs.getNodeId(), lsnr);

            info("Lsnrs: " + lsnrs);

            node.setAttributes(spi.getNodeAttributes());

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
            final ConcurrentMap<UUID, GridTcpCommunicationClient> clients = U.field(spi, "clients");

            assert GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return clients.isEmpty();
                }
            }, getTestTimeout()) : "Clients: " + clients;
        }
    }

    /**
     * @param idx Node index.
     * @return Configured MBean server.
     * @throws Exception If failed.
     */
    private MBeanServer getMBeanServer(int idx) throws Exception {
        HttpAdaptor mbeanAdaptor = new HttpAdaptor();

        MBeanServer mbeanSrv = MBeanServerFactory.createMBeanServer();

        mbeanAdaptor.setPort(
            Integer.valueOf(GridTestProperties.getProperty("comm.mbeanserver.selftest.baseport")) + idx);

        mbeanSrv.registerMBean(mbeanAdaptor, mBeanName);

        mbeanAdaptor.start();

        return mbeanSrv;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (CommunicationSpi<GridTcpCommunicationMessageAdapter> spi : spis.values()) {
            spi.setListener(null);

            spi.spiStop();
        }

        for (GridTestResources rsrcs : spiRsrcs) {
            rsrcs.stopThreads();

            rsrcs.getMBeanServer().unregisterMBean(mBeanName);
        }

        lsnrs.clear();
        spiRsrcs.clear();
        spis.clear();
        nodes.clear();
    }
}
