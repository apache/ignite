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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.continuous.StartRoutineAckDiscoveryMessage;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryStatistics;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.MARSHALLER_PROC;
import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;
import static org.apache.ignite.spi.IgnitePortProtocol.UDP;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
public class TcpDiscoverySelfTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private Map<String, TcpDiscoverySpi> discoMap = new HashMap<>();

    /** */
    private UUID nodeId;

    /** */
    private static ThreadLocal<TcpDiscoverySpi> nodeSpi = new ThreadLocal<>();

    /** */
    private GridStringLogger strLog;

    /** */
    private CacheConfiguration[] ccfgs;

    /** */
    private boolean client;

    /**
     * @throws Exception If fails.
     */
    public TcpDiscoverySelfTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        TcpDiscoverySpi spi = nodeSpi.get();

        if (spi == null) {
            spi = igniteInstanceName.contains("testPingInterruptedOnNodeFailedFailingNode") ?
                new TestTcpDiscoverySpi() : new TcpDiscoverySpi();
        }
        else
            nodeSpi.set(null);

        discoMap.put(igniteInstanceName, spi);

        spi.setIpFinder(ipFinder);

        spi.setNetworkTimeout(2500);

        spi.setIpFinderCleanFrequency(5000);

        spi.setJoinTimeout(5000);

        cfg.setDiscoverySpi(spi);

        cfg.setFailureDetectionTimeout(7500);

        if (ccfgs != null)
            cfg.setCacheConfiguration(ccfgs);
        else
            cfg.setCacheConfiguration();

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cfg.setIncludeProperties();

        cfg.setMetricsUpdateFrequency(1000);

        if (!igniteInstanceName.contains("LoopbackProblemTest"))
            cfg.setLocalHost("127.0.0.1");

        if (igniteInstanceName.contains("testFailureDetectionOnNodePing")) {
            spi.setReconnectCount(1); // To make test faster: on Windows 1 connect takes 1 second.

            cfg.setMetricsUpdateFrequency(40000);
            cfg.setClientFailureDetectionTimeout(41000);
        }

        cfg.setConnectorConfiguration(null);

        if (nodeId != null)
            cfg.setNodeId(nodeId);

        if (igniteInstanceName.contains("NonSharedIpFinder")) {
            TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();

            finder.setAddresses(Arrays.asList("127.0.0.1:47501"));

            spi.setIpFinder(finder);
        }
        else if (igniteInstanceName.contains("MulticastIpFinder")) {
            TcpDiscoveryMulticastIpFinder finder = new TcpDiscoveryMulticastIpFinder();

            finder.setAddressRequestAttempts(5);
            finder.setMulticastGroup(GridTestUtils.getNextMulticastGroup(getClass()));
            finder.setMulticastPort(GridTestUtils.getNextMulticastPort(getClass()));

            spi.setIpFinder(finder);

            // Loopback multicast discovery is not working on Mac OS
            // (possibly due to http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7122846).
            if (U.isMacOs())
                spi.setLocalAddress(F.first(U.allLocalIps()));
        }
        else if (igniteInstanceName.contains("testPingInterruptedOnNodeFailedPingingNode"))
            cfg.setFailureDetectionTimeout(30_000);
        else if (igniteInstanceName.contains("testNoRingMessageWorkerAbnormalFailureNormalNode"))
            cfg.setFailureDetectionTimeout(3_000);
        else if (igniteInstanceName.contains("testNoRingMessageWorkerAbnormalFailureSegmentedNode")) {
            cfg.setFailureDetectionTimeout(6_000);

            cfg.setGridLogger(strLog = new GridStringLogger());

            strLog.logLength(300_000);
        }
        else if (igniteInstanceName.contains("testNodeShutdownOnRingMessageWorkerFailureFailedNode")) {
            cfg.setGridLogger(strLog = new GridStringLogger());

            strLog.logLength(300_000);
        }

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        discoMap = null;

        super.afterTest();
    }

    /**
     * @throws Exception If failed
     */
    public void testNodeShutdownOnRingMessageWorkerStartNotFinished() throws Exception {
        try {
            Ignite ignite0 = startGrid(0);

            TestMessageWorkerFailureSpi2 spi0 = new TestMessageWorkerFailureSpi2();

            nodeSpi.set(spi0);

            try {
                startGrid(1);

                fail();
            }
            catch (Exception e) {
                log.error("Expected error: " + e, e);
            }

            Ignite ignite1 = startGrid(1);

            assertEquals(2, ignite1.cluster().nodes().size());
            assertEquals(4, ignite1.cluster().topologyVersion());

            assertEquals(2, ignite0.cluster().nodes().size());
            assertEquals(4, ignite0.cluster().topologyVersion());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Simulate scenario when node detects node failure trying to send message, but node still alive.
     */
    private static class TestFailedNodesSpi extends TcpDiscoverySpi {
        /** */
        private AtomicBoolean failMsg = new AtomicBoolean();

        /** */
        private int failOrder;

        /** */
        private boolean stopBeforeSndFail;

        /** */
        private boolean stop;

        /** */
        private volatile boolean failSingleMsg;

        /**
         * @param failOrder Spi fails connection if local node order equals to this order.
         */
        TestFailedNodesSpi(int failOrder) {
            this.failOrder = failOrder;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (stop)
                return;

            if (failSingleMsg) {
                failSingleMsg = false;

                log.info("IO error on message send [locNode=" + locNode + ", msg=" + msg + ']');

                sock.close();

                throw new SocketTimeoutException();
            }

            if (locNode.internalOrder() == failOrder &&
                (msg instanceof TcpDiscoveryNodeAddedMessage) &&
                failMsg.compareAndSet(false, true)) {
                log.info("IO error on message send [locNode=" + locNode + ", msg=" + msg + ']');

                sock.close();

                throw new SocketTimeoutException();
            }

            if (stopBeforeSndFail &&
                locNode.internalOrder() == failOrder &&
                (msg instanceof TcpDiscoveryNodeFailedMessage)) {
                stop = true;

                log.info("Skip messages send and stop node [locNode=" + locNode + ", msg=" + msg + ']');

                sock.close();

                GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        ignite.close();

                        return null;
                    }
                }, "stop-node");

                return;
            }

            super.writeToSocket(sock, out, msg, timeout);
        }
    }

    /**
     *
     */
    private static class TestMessageWorkerFailureSpi2 extends TcpDiscoverySpi {
        /** */
        private volatile boolean stop;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (stop)
                throw new RuntimeException("Failing ring message worker explicitly");

            super.writeToSocket(sock, out, msg, timeout);

            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                stop = true;
        }
    }
}
