/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.communication.tcp;

import mx4j.tools.adaptor.http.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.communication.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.spi.*;
import org.jdk8.backport.*;

import javax.management.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Class for multithreaded {@link GridTcpCommunicationSpi} test.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public class GridTcpCommunicationSpiLanTest extends GridSpiAbstractTest<GridTcpCommunicationSpi> {
    /** Connection idle timeout */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** Count of threads sending messages. */
    public static final int THREAD_CNT = 5;

    /** Message id sequence. */
    private AtomicLong msgId = new AtomicLong();

    /** SPI resource. */
    private GridTestResources spiRsrc;

    /** SPI */
    private GridTcpCommunicationSpi spi;

    /** Listener. */
    private MessageListener lsnr;

    /** Local node. */
    private GridTestNode locNode;

    /** Remote node. */
    private GridTestNode remoteNode;

    /** Flag indicating if listener should reject messages. */
    private boolean reject;

    // TODO: change this value to ip of remote node running testRunReceiver
    /** Address of remote machine running receive test. */
    private static final String remoteAddr = "172.1.1.36";

    /** */
    public GridTcpCommunicationSpiLanTest() {
        super(false);
    }

    /**
     * Accumulating listener.
     */
    @SuppressWarnings({"deprecation"})
    private class MessageListener implements CommunicationListener<GridTcpCommunicationMessageAdapter> {
        /** Node id of local node. */
        private final UUID locNodeId;

        /** Received messages by node. */
        private ConcurrentLinkedDeque8<GridTestMessage> rcvdMsgs =
            new ConcurrentLinkedDeque8<>();

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
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }

        /**
         * @return Queue containing received messages in receive order.
         */
        public ConcurrentLinkedDeque8<GridTestMessage> receivedMessages() {
            return rcvdMsgs;
        }

        /**
         * @return Count of messages received from remote node.
         */
        public int remoteMessageCount() {
            return rmtMsgCnt.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunReceiver() throws Exception {
        info(">>> Starting receiving SPI. <<<");

        reject = true;

        while (!Thread.currentThread().isInterrupted()) {
            U.sleep(5000);

            info(">>>" + lsnr.remoteMessageCount() + " messages received from remote node");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunSender() throws Exception {
        reject = true;

        info(">>> Starting send to remote node multithreaded test. <<<");

        final AtomicInteger cntr = new AtomicInteger();

        final int msgCnt = 10000;

        long start = System.currentTimeMillis();

        IgniteFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    while (cntr.getAndIncrement() < msgCnt) {
                        GridTestMessage msg = new GridTestMessage(locNode.id(), msgId.getAndIncrement(), 0);

                        msg.payload(new byte[13 * 1024]);

                        spi.sendMessage(remoteNode, msg);
                    }
                }
                catch (GridException e) {
                    fail("Unable to send message: " + e.getMessage());
                }
            }
        }, THREAD_CNT, "message-sender");

        fut.get();

        info(">>> Sent all messages in " + (System.currentTimeMillis() - start) + " milliseconds");

        assertEquals("Invalid count of messages was sent", msgCnt, msgId.get());

        U.sleep(IDLE_CONN_TIMEOUT * 2);
    }

    /**
     * @return Spi.
     */
    private GridTcpCommunicationSpi createSpi() {
        GridTcpCommunicationSpi spi = new GridTcpCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(IDLE_CONN_TIMEOUT);
        spi.setConnectTimeout(10000);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        spi = createSpi();

        spiRsrc = new GridTestResources(getMBeanServer());

        locNode = new GridTestNode(spiRsrc.getNodeId());

        GridSpiTestContext ctx = initSpiContext();

        ctx.setLocalNode(locNode);

        info(">>> Initialized context: nodeId=" + ctx.localNode().id());

        spiRsrc.inject(spi);

        lsnr = new MessageListener(spiRsrc.getNodeId());

        spi.setListener(lsnr);

        Map<String, Object> attrs = spi.getNodeAttributes();

        locNode.setAttributes(attrs);

        spi.spiStart(getTestGridName());

        spi.onContextInitialized(ctx);

        GridTestResources remoteRsrc = new GridTestResources();

        remoteNode = new GridTestNode(remoteRsrc.getNodeId());

        remoteNode.setAttributes(attrs);

        remoteNode.setAttribute(U.spiAttribute(spi, GridTcpCommunicationSpi.ATTR_ADDRS),
            Collections.singleton(remoteAddr));

        ctx.remoteNodes().add(remoteNode);
    }

    /**
     * @return Configured MBean server.
     * @throws Exception If failed.
     */
    private MBeanServer getMBeanServer() throws Exception {
        HttpAdaptor mbeanAdaptor = new HttpAdaptor();

        MBeanServer mbeanSrv = MBeanServerFactory.createMBeanServer();

        mbeanAdaptor.setPort(
            Integer.valueOf(GridTestProperties.getProperty("comm.mbeanserver.selftest.baseport")));

        mbeanSrv.registerMBean(mbeanAdaptor, new ObjectName("mbeanAdaptor:protocol=HTTP"));

        mbeanAdaptor.start();

        return mbeanSrv;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"NullableProblems"})
    @Override protected void afterTestsStopped() throws Exception {
        spi.setListener(null);

        spi.spiStop();

        spiRsrc.stopThreads();

        tearDown();
    }

    /**
     * @return 0 to disable test timeouts.
     */
    @Override protected long getTestTimeout() {
        return 0;
    }
}
