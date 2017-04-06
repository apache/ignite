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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import mx4j.tools.adaptor.http.HttpAdaptor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Class for multithreaded {@link TcpCommunicationSpi} test.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public class GridTcpCommunicationSpiLanTest extends GridSpiAbstractTest<TcpCommunicationSpi> {
    /** Connection idle timeout */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** Count of threads sending messages. */
    public static final int THREAD_CNT = 5;

    /** Message id sequence. */
    private AtomicLong msgId = new AtomicLong();

    /** SPI resource. */
    private IgniteTestResources spiRsrc;

    /** SPI */
    private TcpCommunicationSpi spi;

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
    private class MessageListener implements CommunicationListener<Message> {
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

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    while (cntr.getAndIncrement() < msgCnt) {
                        GridTestMessage msg = new GridTestMessage(locNode.id(), msgId.getAndIncrement(), 0);

                        msg.payload(new byte[13 * 1024]);

                        spi.sendMessage(remoteNode, msg);
                    }
                }
                catch (IgniteException e) {
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
    private TcpCommunicationSpi createSpi() {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(IDLE_CONN_TIMEOUT);
        spi.setConnectTimeout(10000);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        spi = createSpi();

        spiRsrc = new IgniteTestResources(getMBeanServer());

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

        IgniteTestResources remoteRsrc = new IgniteTestResources();

        remoteNode = new GridTestNode(remoteRsrc.getNodeId());

        remoteNode.setAttributes(attrs);

        remoteNode.setAttribute(U.spiAttribute(spi, TcpCommunicationSpi.ATTR_ADDRS),
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